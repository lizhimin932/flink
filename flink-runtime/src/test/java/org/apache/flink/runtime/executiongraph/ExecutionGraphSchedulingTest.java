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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.concurrent.impl.FlinkCompletableFuture;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.restart.NoRestartStrategy;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.instance.Slot;
import org.apache.flink.runtime.instance.SlotProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.scheduler.ScheduledUnit;
import org.apache.flink.runtime.jobmanager.scheduler.Scheduler;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmanager.slots.ActorTaskManagerGateway;
import org.apache.flink.runtime.jobmanager.slots.AllocatedSlot;
import org.apache.flink.runtime.jobmanager.slots.SlotOwner;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;


import org.junit.After;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.Timeout;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the scheduling of the execution graph. This tests that
 * for example the order of deployments is correct and that bulk slot allocation
 * works properly.
 */
public class ExecutionGraphSchedulingTest extends TestLogger {

	private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

	@After
	public void shutdown() {
		executor.shutdownNow();
	}

	// ------------------------------------------------------------------------
	//  Tests
	// ------------------------------------------------------------------------

	/**
	 * Tests that with scheduling futures and pipelined deployment, the target vertex will
	 * not deploy its task before the source vertex does.
	 */
	@Test
	public void testScheduleSourceBeforeTarget() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 1;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);

		final FlinkCompletableFuture<SimpleSlot> sourceFuture = new FlinkCompletableFuture<>();
		final FlinkCompletableFuture<SimpleSlot> targetFuture = new FlinkCompletableFuture<>();

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlot(sourceVertex.getID(), 0, sourceFuture);
		slotProvider.addSlot(targetVertex.getID(), 0, targetFuture);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		//  set up two TaskManager gateways and slots

		final TaskManagerGateway gatewaySource = createTaskManager();
		final TaskManagerGateway gatewayTarget = createTaskManager();

		final SimpleSlot sourceSlot = createSlot(gatewaySource, jobId);
		final SimpleSlot targetSlot = createSlot(gatewayTarget, jobId);

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.setQueuedSchedulingAllowed(true);
		eg.scheduleForExecution();

		// job should be running
		assertEquals(JobStatus.RUNNING, eg.getState());

		// we fulfill the target slot before the source slot
		// that should not cause a deployment or deployment related failure
		targetFuture.complete(targetSlot);

		verify(gatewayTarget, new Timeout(50, times(0))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		assertEquals(JobStatus.RUNNING, eg.getState());

		// now supply the source slot
		sourceFuture.complete(sourceSlot);

		// by now, all deployments should have happened
		verify(gatewaySource, timeout(1000)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		verify(gatewayTarget, timeout(1000)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		assertEquals(JobStatus.RUNNING, eg.getState());
	}

	/**
	 * This test verifies that before deploying a pipelined connected component, the
	 * full set of slots is available, and that not some tasks are deployed, and later the
	 * system realizes that not enough resources are available.
	 */
	@Test
	public void testDeployPipelinedConnectedComponentsTogether() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 8;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);

		@SuppressWarnings({"unchecked", "rawtypes"})
		final FlinkCompletableFuture<SimpleSlot>[] sourceFutures = new FlinkCompletableFuture[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final FlinkCompletableFuture<SimpleSlot>[] targetFutures = new FlinkCompletableFuture[parallelism];

		//
		//  Create the slots, futures, and the slot provider

		final TaskManagerGateway[] sourceTaskManagers = new TaskManagerGateway[parallelism];
		final TaskManagerGateway[] targetTaskManagers = new TaskManagerGateway[parallelism];

		final SimpleSlot[] sourceSlots = new SimpleSlot[parallelism];
		final SimpleSlot[] targetSlots = new SimpleSlot[parallelism];

		for (int i = 0; i < parallelism; i++) {
			sourceTaskManagers[i] = createTaskManager();
			targetTaskManagers[i] = createTaskManager();

			sourceSlots[i] = createSlot(sourceTaskManagers[i], jobId);
			targetSlots[i] = createSlot(targetTaskManagers[i], jobId);

			sourceFutures[i] = new FlinkCompletableFuture<>();
			targetFutures[i] = new FlinkCompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(sourceVertex.getID(), sourceFutures);
		slotProvider.addSlots(targetVertex.getID(), targetFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		//
		//  we complete some of the futures

		for (int i = 0; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
		}

		//
		//  kick off the scheduling

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.setQueuedSchedulingAllowed(true);
		eg.scheduleForExecution();

		verifyNothingDeployed(eg, sourceTaskManagers);

		//  complete the remaining sources
		for (int i = 1; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
		}
		verifyNothingDeployed(eg, sourceTaskManagers);

		//  complete the targets except for one
		for (int i = 1; i < parallelism; i++) {
			targetFutures[i].complete(targetSlots[i]);
		}
		verifyNothingDeployed(eg, targetTaskManagers);

		//  complete the last target slot future
		targetFutures[0].complete(targetSlots[0]);

		//
		//  verify that all deployments have happened

		for (TaskManagerGateway gateway : sourceTaskManagers) {
			verify(gateway, timeout(500L)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		}
		for (TaskManagerGateway gateway : targetTaskManagers) {
			verify(gateway, timeout(500L)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		}
	}

	/**
	 * This test verifies that if one slot future fails, the deployment will be aborted.
	 */
	@Test
	public void testOneSlotFailureAbortsDeploy() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 6;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);

		//
		//  Create the slots, futures, and the slot provider

		final TaskManagerGateway taskManager = mock(TaskManagerGateway.class);
		final SlotOwner slotOwner = mock(SlotOwner.class);

		final SimpleSlot[] sourceSlots = new SimpleSlot[parallelism];
		final SimpleSlot[] targetSlots = new SimpleSlot[parallelism];

		@SuppressWarnings({"unchecked", "rawtypes"})
		final FlinkCompletableFuture<SimpleSlot>[] sourceFutures = new FlinkCompletableFuture[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final FlinkCompletableFuture<SimpleSlot>[] targetFutures = new FlinkCompletableFuture[parallelism];

		for (int i = 0; i < parallelism; i++) {
			sourceSlots[i] = createSlot(taskManager, jobId, slotOwner);
			targetSlots[i] = createSlot(taskManager, jobId, slotOwner);

			sourceFutures[i] = new FlinkCompletableFuture<>();
			targetFutures[i] = new FlinkCompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(sourceVertex.getID(), sourceFutures);
		slotProvider.addSlots(targetVertex.getID(), targetFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		//
		//  we complete some of the futures

		for (int i = 0; i < parallelism; i += 2) {
			sourceFutures[i].complete(sourceSlots[i]);
			targetFutures[i + 1].complete(targetSlots[i + 1]);
		}

		//
		//  kick off the scheduling

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.setQueuedSchedulingAllowed(true);
		eg.scheduleForExecution();

		// fail one slot
		sourceFutures[1].completeExceptionally(new TestRuntimeException());

		// wait until the job failed as a whole
		eg.getTerminationFuture().get(2000, TimeUnit.MILLISECONDS);

		// wait until all slots are back
		verify(slotOwner, new Timeout(2000, times(6))).returnAllocatedSlot(any(Slot.class));

		// no deployment calls must have happened
		verify(taskManager, times(0)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		// all completed futures must have been returns
		for (int i = 0; i < parallelism; i += 2) {
			assertTrue(sourceSlots[i].isCanceled());
			assertTrue(targetSlots[i + 1].isCanceled());
		}
	}

	/**
	 * This test verifies that the slot allocations times out after a certain time, and that
	 * all slots are released in that case.
	 */
	@Test
	public void testTimeoutForSlotAllocation() throws Exception {

		//  we construct a simple graph:    (task)

		final int parallelism = 3;

		final JobVertex vertex = new JobVertex("task");
		vertex.setParallelism(parallelism);
		vertex.setInvokableClass(NoOpInvokable.class);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", vertex);

		final SlotOwner slotOwner = mock(SlotOwner.class);

		final TaskManagerGateway taskManager = mock(TaskManagerGateway.class);
		final SimpleSlot[] slots = new SimpleSlot[parallelism];
		@SuppressWarnings({"unchecked", "rawtypes"})
		final FlinkCompletableFuture<SimpleSlot>[] slotFutures = new FlinkCompletableFuture[parallelism];

		for (int i = 0; i < parallelism; i++) {
			slots[i] = createSlot(taskManager, jobId, slotOwner);
			slotFutures[i] = new FlinkCompletableFuture<>();
		}

		ProgrammedSlotProvider slotProvider = new ProgrammedSlotProvider(parallelism);
		slotProvider.addSlots(vertex.getID(), slotFutures);

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider, Time.milliseconds(20));

		//  we complete one future
		slotFutures[1].complete(slots[1]);

		//  kick off the scheduling

		eg.setScheduleMode(ScheduleMode.EAGER);
		eg.setQueuedSchedulingAllowed(true);
		eg.scheduleForExecution();

		//  we complete another future
		slotFutures[2].complete(slots[2]);

		// since future[0] is still missing the while operation must time out
		// we have no restarts allowed, so the job will go terminal
		eg.getTerminationFuture().get(2000, TimeUnit.MILLISECONDS);

		// wait until all slots are back
		verify(slotOwner, new Timeout(2000, times(2))).returnAllocatedSlot(any(Slot.class));

		//  verify that no deployments have happened
		verify(taskManager, times(0)).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));

		for (Future<SimpleSlot> future : slotFutures) {
			if (future.isDone()) {
				assertTrue(future.get().isCanceled());
			}
		}
	}

	/**
	 * Tests that the {@link ExecutionJobVertex#allocateResourcesForAll(SlotProvider, boolean)} method
	 * releases partially acquired resources upon exception.
	 */
	@Test
	public void testExecutionJobVertexAllocateResourcesReleasesOnException() throws Exception {
		final int parallelism = 8;

		final JobVertex vertex = new JobVertex("vertex");
		vertex.setParallelism(parallelism);
		vertex.setInvokableClass(NoOpInvokable.class);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", vertex);

		// set up some available slots and some slot owner that accepts released slots back
		final List<SimpleSlot> returnedSlots = new ArrayList<>();
		final SlotOwner recycler = new SlotOwner() {
			@Override
			public boolean returnAllocatedSlot(Slot slot) {
				returnedSlots.add((SimpleSlot) slot);
				return true;
			}
		};

		// slot provider that hand out parallelism / 3 slots, then throws an exception
		final SlotProvider slotProvider = mock(SlotProvider.class);

		final TaskManagerGateway taskManager = mock(TaskManagerGateway.class);
		final List<SimpleSlot> availableSlots = new ArrayList<>(Arrays.asList(
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler)));

		when(slotProvider.allocateSlot(any(ScheduledUnit.class), anyBoolean())).then(
			new Answer<Future<SimpleSlot>>() {

				@Override
				public Future<SimpleSlot> answer(InvocationOnMock invocation) {
					if (availableSlots.isEmpty()) {
						throw new TestRuntimeException();
					} else {
						return FlinkCompletableFuture.completed(availableSlots.remove(0));
					}
				}
			});

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);
		final ExecutionJobVertex ejv = eg.getJobVertex(vertex.getID());

		// acquire resources and check that all are back after the failure

		final int numSlotsToExpectBack = availableSlots.size();

		try {
			ejv.allocateResourcesForAll(slotProvider, false);
			fail("should have failed with an exception");
		}
		catch (TestRuntimeException e) {
			// expected
		}

		assertEquals(numSlotsToExpectBack, returnedSlots.size());
	}

	/**
	 * Tests that the {@link ExecutionGraph#scheduleForExecution()} method
	 * releases partially acquired resources upon exception.
	 */
	@Test
	public void testExecutionGraphScheduleReleasesResourcesOnException() throws Exception {

		//                                            [pipelined]
		//  we construct a simple graph    (source) ----------------> (target)

		final int parallelism = 3;

		final JobVertex sourceVertex = new JobVertex("source");
		sourceVertex.setParallelism(parallelism);
		sourceVertex.setInvokableClass(NoOpInvokable.class);

		final JobVertex targetVertex = new JobVertex("target");
		targetVertex.setParallelism(parallelism);
		targetVertex.setInvokableClass(NoOpInvokable.class);

		targetVertex.connectNewDataSetAsInput(sourceVertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

		final JobID jobId = new JobID();
		final JobGraph jobGraph = new JobGraph(jobId, "test", sourceVertex, targetVertex);

		// set up some available slots and some slot owner that accepts released slots back
		final List<SimpleSlot> returnedSlots = new ArrayList<>();
		final SlotOwner recycler = new SlotOwner() {
			@Override
			public boolean returnAllocatedSlot(Slot slot) {
				returnedSlots.add((SimpleSlot) slot);
				return true;
			}
		};

		final TaskManagerGateway taskManager = mock(TaskManagerGateway.class);
		final List<SimpleSlot> availableSlots = new ArrayList<>(Arrays.asList(
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler),
			createSlot(taskManager, jobId, recycler)));


		// slot provider that hand out parallelism / 3 slots, then throws an exception
		final SlotProvider slotProvider = mock(SlotProvider.class);

		when(slotProvider.allocateSlot(any(ScheduledUnit.class), anyBoolean())).then(
			new Answer<Future<SimpleSlot>>() {

				@Override
				public Future<SimpleSlot> answer(InvocationOnMock invocation) {
					if (availableSlots.isEmpty()) {
						throw new TestRuntimeException();
					} else {
						return FlinkCompletableFuture.completed(availableSlots.remove(0));
					}
				}
			});

		final ExecutionGraph eg = createExecutionGraph(jobGraph, slotProvider);

		// acquire resources and check that all are back after the failure

		final int numSlotsToExpectBack = availableSlots.size();

		try {
			eg.setScheduleMode(ScheduleMode.EAGER);
			eg.scheduleForExecution();
			fail("should have failed with an exception");
		}
		catch (TestRuntimeException e) {
			// expected
		}

		assertEquals(numSlotsToExpectBack, returnedSlots.size());
	}

	/**
	 * Tests that {@link ExecutionVertex#getPreferredLocationsBasedOnInputs(()} method
	 * return the preferred location base on inputs before deploy to the TM.
	 */
	@Test
	public void testExecutionVertexGetPreferredLocationsBasedOnInputs() {
		try {
			final JobID jobId = new JobID();

			final JobVertexID source1Id = new JobVertexID();
			final JobVertexID source2Id = new JobVertexID();
			final JobVertexID target1Id = new JobVertexID();
			final JobVertexID target2Id = new JobVertexID();

			JobVertex source1 = new JobVertex("source1", source1Id);
			JobVertex source2 = new JobVertex("source2", source2Id);
			JobVertex target1 = new JobVertex("target1", target1Id);
			JobVertex target2 = new JobVertex("target2", target2Id);

			source1.setParallelism(2);
			source2.setParallelism(4);
			target1.setParallelism(1);
			target2.setParallelism(2);

			SlotSharingGroup group = new SlotSharingGroup();
			source1.setSlotSharingGroup(group);
			source2.setSlotSharingGroup(group);
			target1.setSlotSharingGroup(group);
			target2.setSlotSharingGroup(group);

			source1.setInvokableClass(NoOpInvokable.class);
			source2.setInvokableClass(NoOpInvokable.class);
			target1.setInvokableClass(NoOpInvokable.class);
			target2.setInvokableClass(NoOpInvokable.class);

			// construct a simple graph as below (DistributionPattern's POINTWISE)
			//
			// source1 (parallelism 2) ---+-> target1 (parallelism1)
			//                           /
			//                          /
			// source2 (parallelism 4) +--->target2 (parallelism2)
			target1.connectNewDataSetAsInput(source1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);
			target1.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);
			target2.connectNewDataSetAsInput(source2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED_BOUNDED);

			final JobGraph jobGraph = new JobGraph(jobId, "test", source1, source2, target1, target2);

			// build scheduler and add 4 instances, every instance with 1 slot.
			Scheduler scheduler = new Scheduler(TestingUtils.directExecutionContext());
			for (int i = 0; i < 4; i++) {
				scheduler.newInstanceAvailable(
					ExecutionGraphTestUtils.getInstance(
						new ActorTaskManagerGateway(
							new ExecutionGraphTestUtils.SimpleActorGateway(
								TestingUtils.directExecutionContext()))));
			}

			ExecutionGraph eg = createExecutionGraph(jobGraph, scheduler);

			// allocate the slots
			for (ExecutionJobVertex ejv : eg.getVerticesTopologically()) {
				ejv.allocateResourcesForAll(scheduler, false);
			}

			Map<JobVertexID, ExecutionJobVertex> ejvMap = eg.getAllVertices();

			// source1 & source2 should have no one preferred location based on inputs
			assertTrue(!ejvMap.get(source1Id).getTaskVertices()[0].getPreferredLocationsBasedOnInputs().iterator().hasNext());
			assertTrue(!ejvMap.get(source2Id).getTaskVertices()[0].getPreferredLocationsBasedOnInputs().iterator().hasNext());

			// target1 should have 2 preferred locations
			Set<TaskManagerLocation> target1PreferredSet = new HashSet<>();
			for(TaskManagerLocation tmLocation: ejvMap.get(target1Id).getTaskVertices()[0].getPreferredLocationsBasedOnInputs()) {
				target1PreferredSet.add(tmLocation);
			}
			// target1's preferred locations should be the same as source1's slot locations
			assertTrue(target1PreferredSet.size() == 2);
			assertTrue(target1PreferredSet.contains(ejvMap.get(source1Id).getTaskVertices()[0].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));
			assertTrue(target1PreferredSet.contains(ejvMap.get(source1Id).getTaskVertices()[1].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));

			// target2's 1th sub vertex should have 2 preferred inputs
			Set<TaskManagerLocation> target2Vertex1PreferredSet = new HashSet<>();
			for(TaskManagerLocation tmLocation: ejvMap.get(target2Id).getTaskVertices()[0].getPreferredLocationsBasedOnInputs()) {
				target2Vertex1PreferredSet.add(tmLocation);
			}
			assertTrue(target2Vertex1PreferredSet.size() == 2);

			// target2's 2th sub vertex should have 2 preferred inputs
			Set<TaskManagerLocation> target2Vertex2PreferredSet = new HashSet<>();
			for(TaskManagerLocation tmLocation: ejvMap.get(target2Id).getTaskVertices()[1].getPreferredLocationsBasedOnInputs()) {
				target2Vertex2PreferredSet.add(tmLocation);
			}
			assertTrue(target2Vertex2PreferredSet.size() == 2);

			// target2's preferred locations should be the same as source2's slot locations
			Set<TaskManagerLocation> target2VertexPreferredSet = new HashSet<>();
			target2VertexPreferredSet.addAll(target2Vertex1PreferredSet);
			target2VertexPreferredSet.addAll(target2Vertex2PreferredSet);

			assertTrue(target2VertexPreferredSet.contains(ejvMap.get(source2Id).getTaskVertices()[0].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));
			assertTrue(target2VertexPreferredSet.contains(ejvMap.get(source2Id).getTaskVertices()[1].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));
			assertTrue(target2VertexPreferredSet.contains(ejvMap.get(source2Id).getTaskVertices()[2].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));
			assertTrue(target2VertexPreferredSet.contains(ejvMap.get(source2Id).getTaskVertices()[3].getCurrentExecutionAttempt().getAssignedFutureResource().getTaskManagerLocation()));
		} catch (Exception e) {
			fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider) throws Exception {
		return createExecutionGraph(jobGraph, slotProvider, Time.minutes(10));
	}

	private ExecutionGraph createExecutionGraph(JobGraph jobGraph, SlotProvider slotProvider, Time timeout) throws Exception {
		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			new Configuration(),
			executor,
			executor,
			slotProvider,
			getClass().getClassLoader(),
			new StandaloneCheckpointRecoveryFactory(),
			timeout,
			new NoRestartStrategy(),
			new UnregisteredMetricsGroup(),
			1,
			log);
	}

	private SimpleSlot createSlot(TaskManagerGateway taskManager, JobID jobId) {
		return createSlot(taskManager, jobId, mock(SlotOwner.class));
	}

	private SimpleSlot createSlot(TaskManagerGateway taskManager, JobID jobId, SlotOwner slotOwner) {
		TaskManagerLocation location = new TaskManagerLocation(
				ResourceID.generate(), InetAddress.getLoopbackAddress(), 12345);

		AllocatedSlot slot = new AllocatedSlot(
				new AllocationID(), jobId, location, 0, ResourceProfile.UNKNOWN, taskManager);

		return new SimpleSlot(slot, slotOwner, 0);
	}

	private static TaskManagerGateway createTaskManager() {
		TaskManagerGateway tm = mock(TaskManagerGateway.class);
		when(tm.submitTask(any(TaskDeploymentDescriptor.class), any(Time.class)))
				.thenReturn(FlinkCompletableFuture.completed(Acknowledge.get()));

		return tm;
	}

	private static void verifyNothingDeployed(ExecutionGraph eg, TaskManagerGateway[] taskManagers) {
		// job should still be running
		assertEquals(JobStatus.RUNNING, eg.getState());

		// none of the TaskManager should have gotten a deployment call, yet
		for (TaskManagerGateway gateway : taskManagers) {
			verify(gateway, new Timeout(50, times(0))).submitTask(any(TaskDeploymentDescriptor.class), any(Time.class));
		}
	}

	private static class TestRuntimeException extends RuntimeException {
		private static final long serialVersionUID = 1L;
	}
}
