/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridge;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolUtils;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

/** Tests the restart behaviour of the {@link ExecutionGraph}. */
public class ExecutionGraphRestartTest extends TestLogger {

    private static final int NUM_TASKS = 31;

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final ComponentMainThreadExecutor mainThreadExecutor =
            ComponentMainThreadExecutorServiceAdapter.forMainThread();

    private ManuallyTriggeredScheduledExecutor taskRestartExecutor;

    @Before
    public void setUp() {
        taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    // ------------------------------------------------------------------------

    private void completeCanceling(ExecutionGraph eg) {
        executeOperationForAllExecutions(eg, Execution::completeCancelling);
    }

    private void executeOperationForAllExecutions(
            ExecutionGraph eg, Consumer<Execution> operation) {
        for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
            operation.accept(vertex.getCurrentExecutionAttempt());
        }
    }

    @Test
    public void testCancelAllPendingRequestWhileCanceling() throws Exception {
        try (DeclarativeSlotPoolBridge slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {

            final int numTasksExceedSlotPool = 50;
            // create a graph with task count larger than slot pool
            JobVertex sender =
                    ExecutionGraphTestUtils.createJobVertex(
                            "Task", NUM_TASKS + numTasksExceedSlotPool, NoOpInvokable.class);
            JobGraph graph = JobGraphTestUtils.streamingJobGraph(sender);
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    graph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .build();
            ExecutionGraph executionGraph = scheduler.getExecutionGraph();

            startScheduling(scheduler);
            offerSlots(slotPool, NUM_TASKS);

            assertEquals(numTasksExceedSlotPool, slotPool.getNumPendingRequests());

            scheduler.cancel();
            assertEquals(JobStatus.CANCELLING, executionGraph.getState());
            assertEquals(0, slotPool.getNumPendingRequests());
        }
    }

    @Test
    public void testCancelAllPendingRequestWhileFailing() throws Exception {
        try (DeclarativeSlotPoolBridge slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {

            final int numTasksExceedSlotPool = 50;
            // create a graph with task count larger than slot pool
            JobVertex sender =
                    ExecutionGraphTestUtils.createJobVertex(
                            "Task", NUM_TASKS + numTasksExceedSlotPool, NoOpInvokable.class);
            JobGraph graph = JobGraphTestUtils.streamingJobGraph(sender);
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    graph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .build();
            ExecutionGraph executionGraph = scheduler.getExecutionGraph();

            startScheduling(scheduler);
            offerSlots(slotPool, NUM_TASKS);

            assertEquals(numTasksExceedSlotPool, slotPool.getNumPendingRequests());

            scheduler.handleGlobalFailure(new Exception("test"));
            assertEquals(JobStatus.FAILING, executionGraph.getState());
            assertEquals(0, slotPool.getNumPendingRequests());
        }
    }

    @Test
    public void testCancelWhileRestarting() throws Exception {
        // We want to manually control the restart and delay
        try (SlotPool slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    createJobGraph(),
                                    mainThreadExecutor,
                                    EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .setRestartBackoffTimeStrategy(
                                    new TestRestartBackoffTimeStrategy(true, Long.MAX_VALUE))
                            .setDelayExecutor(taskRestartExecutor)
                            .build();
            ExecutionGraph executionGraph = scheduler.getExecutionGraph();

            startScheduling(scheduler);

            final ResourceID taskManagerResourceId = offerSlots(slotPool, NUM_TASKS);

            // Release the TaskManager and wait for the job to restart
            slotPool.releaseTaskManager(taskManagerResourceId, new Exception("Test Exception"));
            assertEquals(JobStatus.RESTARTING, executionGraph.getState());

            // Canceling needs to abort the restart
            scheduler.cancel();

            assertEquals(JobStatus.CANCELED, executionGraph.getState());

            taskRestartExecutor.triggerScheduledTasks();

            assertEquals(JobStatus.CANCELED, executionGraph.getState());
            for (ExecutionVertex vertex : executionGraph.getAllExecutionVertices()) {
                assertEquals(ExecutionState.FAILED, vertex.getExecutionState());
            }
        }
    }

    private static ResourceID offerSlots(SlotPool slotPool, int numSlots) {
        return SlotPoolUtils.offerSlots(
                slotPool, mainThreadExecutor, Collections.nCopies(numSlots, ResourceProfile.ANY));
    }

    @Test
    public void testCancelWhileFailing() throws Exception {
        try (SlotPool slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    createJobGraph(),
                                    mainThreadExecutor,
                                    EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .setRestartBackoffTimeStrategy(
                                    new TestRestartBackoffTimeStrategy(false, Long.MAX_VALUE))
                            .build();
            ExecutionGraph graph = scheduler.getExecutionGraph();

            startScheduling(scheduler);

            offerSlots(slotPool, NUM_TASKS);

            assertEquals(JobStatus.RUNNING, graph.getState());

            switchAllTasksToRunning(graph);

            scheduler.handleGlobalFailure(new Exception("test"));

            assertEquals(JobStatus.FAILING, graph.getState());

            scheduler.cancel();

            assertEquals(JobStatus.CANCELLING, graph.getState());

            // let all tasks finish cancelling
            completeCanceling(graph);

            assertEquals(JobStatus.CANCELED, graph.getState());
        }
    }

    @Test
    public void testFailWhileCanceling() throws Exception {
        try (SlotPool slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    createJobGraph(),
                                    mainThreadExecutor,
                                    EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .setRestartBackoffTimeStrategy(
                                    new TestRestartBackoffTimeStrategy(false, Long.MAX_VALUE))
                            .build();
            ExecutionGraph graph = scheduler.getExecutionGraph();

            startScheduling(scheduler);

            offerSlots(slotPool, NUM_TASKS);

            assertEquals(JobStatus.RUNNING, graph.getState());
            switchAllTasksToRunning(graph);

            scheduler.cancel();

            assertEquals(JobStatus.CANCELLING, graph.getState());

            scheduler.handleGlobalFailure(new Exception("test"));

            assertEquals(JobStatus.FAILING, graph.getState());

            // let all tasks finish cancelling
            completeCanceling(graph);

            assertEquals(JobStatus.FAILED, graph.getState());
        }
    }

    private void switchAllTasksToRunning(ExecutionGraph graph) {
        executeOperationForAllExecutions(graph, Execution::switchToRunning);
    }

    /**
     * Tests that a failing execution does not affect a restarted job. This is important if a
     * callback handler fails an execution after it has already reached a final state and the job
     * has been restarted.
     */
    @Test
    public void testFailingExecutionAfterRestart() throws Exception {
        JobVertex sender = ExecutionGraphTestUtils.createJobVertex("Task1", 1, NoOpInvokable.class);
        JobVertex receiver =
                ExecutionGraphTestUtils.createJobVertex("Task2", 1, NoOpInvokable.class);
        JobGraph jobGraph = JobGraphTestUtils.streamingJobGraph(sender, receiver);

        try (SlotPool slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .setRestartBackoffTimeStrategy(
                                    new TestRestartBackoffTimeStrategy(true, Long.MAX_VALUE))
                            .setDelayExecutor(taskRestartExecutor)
                            .build();
            ExecutionGraph eg = scheduler.getExecutionGraph();

            startScheduling(scheduler);

            offerSlots(slotPool, 2);

            Iterator<ExecutionVertex> executionVertices = eg.getAllExecutionVertices().iterator();

            Execution finishedExecution = executionVertices.next().getCurrentExecutionAttempt();
            Execution failedExecution = executionVertices.next().getCurrentExecutionAttempt();

            finishedExecution.markFinished();

            failedExecution.fail(new Exception("Test Exception"));
            failedExecution.completeCancelling();

            taskRestartExecutor.triggerScheduledTasks();

            assertEquals(JobStatus.RUNNING, eg.getState());

            // At this point all resources have been assigned
            for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
                assertNotNull(
                        "No assigned resource (test instability).",
                        vertex.getCurrentAssignedResource());
                vertex.getCurrentExecutionAttempt().switchToRecovering();
                vertex.getCurrentExecutionAttempt().switchToRunning();
            }

            // fail old finished execution, this should not affect the execution
            finishedExecution.fail(new Exception("This should have no effect"));

            for (ExecutionVertex vertex : eg.getAllExecutionVertices()) {
                vertex.getCurrentExecutionAttempt().markFinished();
            }

            // the state of the finished execution should have not changed since it is terminal
            assertEquals(ExecutionState.FINISHED, finishedExecution.getState());

            assertEquals(JobStatus.FINISHED, eg.getState());
        }
    }

    /**
     * Tests that a graph is not restarted after cancellation via a call to {@link
     * Execution#fail(Throwable)}. This can happen when a slot is released concurrently with
     * cancellation.
     */
    @Test
    public void testFailExecutionAfterCancel() throws Exception {
        try (SlotPool slotPool = SlotPoolUtils.createDeclarativeSlotPoolBridge()) {
            SchedulerBase scheduler =
                    new SchedulerTestingUtils.DefaultSchedulerBuilder(
                                    createJobGraphToCancel(),
                                    mainThreadExecutor,
                                    EXECUTOR_RESOURCE.getExecutor())
                            .setExecutionSlotAllocatorFactory(
                                    createExecutionSlotAllocatorFactory(slotPool))
                            .setRestartBackoffTimeStrategy(
                                    new TestRestartBackoffTimeStrategy(false, Long.MAX_VALUE))
                            .setDelayExecutor(taskRestartExecutor)
                            .build();
            ExecutionGraph eg = scheduler.getExecutionGraph();

            startScheduling(scheduler);

            offerSlots(slotPool, 1);

            // Fail right after cancel (for example with concurrent slot release)
            scheduler.cancel();

            for (ExecutionVertex v : eg.getAllExecutionVertices()) {
                v.getCurrentExecutionAttempt().fail(new Exception("Test Exception"));
            }

            assertEquals(JobStatus.CANCELED, eg.getTerminationFuture().get());

            Execution execution =
                    eg.getAllExecutionVertices().iterator().next().getCurrentExecutionAttempt();

            execution.completeCancelling();
            assertEquals(JobStatus.CANCELED, eg.getState());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static void startScheduling(SchedulerBase scheduler) {
        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.CREATED));
        scheduler.startScheduling();
        assertThat(scheduler.getExecutionGraph().getState(), is(JobStatus.RUNNING));
    }

    private static ExecutionSlotAllocatorFactory createExecutionSlotAllocatorFactory(
            SlotPool slotPool) throws Exception {
        setupSlotPool(slotPool);
        PhysicalSlotProvider physicalSlotProvider =
                new PhysicalSlotProviderImpl(
                        LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
        return SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                physicalSlotProvider);
    }

    private static void setupSlotPool(SlotPool slotPool) throws Exception {
        final String jobManagerAddress = "foobar";
        final ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
        slotPool.start(JobMasterId.generate(), jobManagerAddress, mainThreadExecutor);
        slotPool.connectToResourceManager(resourceManagerGateway);
    }

    private static JobGraph createJobGraph() {
        JobVertex sender =
                ExecutionGraphTestUtils.createJobVertex("Task", NUM_TASKS, NoOpInvokable.class);
        return JobGraphTestUtils.streamingJobGraph(sender);
    }

    private static JobGraph createJobGraphToCancel() throws IOException {
        JobVertex vertex =
                ExecutionGraphTestUtils.createJobVertex("Test Vertex", 1, NoOpInvokable.class);
        ExecutionConfig executionConfig = new ExecutionConfig();
        executionConfig.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, Integer.MAX_VALUE));

        return JobGraphBuilder.newStreamingJobGraphBuilder()
                .addJobVertex(vertex)
                .setExecutionConfig(executionConfig)
                .build();
    }
}
