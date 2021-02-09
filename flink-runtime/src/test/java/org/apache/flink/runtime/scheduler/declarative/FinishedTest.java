/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

/** Tests for declarative scheduler's {@link Finished} state. */
public class FinishedTest extends TestLogger {

    @Test
    public void testOnFinishedCallOnEnter() throws Exception {
        MockFinishedContext ctx = new MockFinishedContext();
        Finished finished = createFinishedState(ctx);
        finished.onEnter();

        assertThat(ctx.getArchivedExecutionGraph().getState(), is(JobStatus.FAILED));
    }

    @Test
    public void testCancelIgnored() throws Exception {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx).cancel();
        ctx.assertNoStateTransition();
    }

    @Test
    public void testSuspendIgnored() throws Exception {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx).suspend(new RuntimeException());
        ctx.assertNoStateTransition();
    }

    @Test
    public void testGlobalFailureIgnored() throws Exception {
        MockFinishedContext ctx = new MockFinishedContext();
        createFinishedState(ctx).handleGlobalFailure(new RuntimeException());
        ctx.assertNoStateTransition();
    }

    @Test
    public void testGetJobStatus() throws Exception {
        MockFinishedContext ctx = new MockFinishedContext();
        assertThat(createFinishedState(ctx).getJobStatus(), is(JobStatus.FAILED));
    }

    private Finished createFinishedState(MockFinishedContext ctx)
            throws JobException, JobExecutionException {
        // put in FAILED state to test against
        final ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        executionGraph.failJob(new RuntimeException());
        final ArchivedExecutionGraph archivedExecutionGraph =
                ArchivedExecutionGraph.createFrom(executionGraph);
        return new Finished(ctx, archivedExecutionGraph, log);
    }

    private static class MockFinishedContext implements Finished.Context {

        private ArchivedExecutionGraph archivedExecutionGraph = null;

        @Override
        public void onFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            if (archivedExecutionGraph != null) {
                this.archivedExecutionGraph = archivedExecutionGraph;
            } else {
                throw new AssertionError("Transitioned to onFinished twice");
            }
        }

        private void assertNoStateTransition() {
            assertThat(archivedExecutionGraph, nullValue());
        }

        private ArchivedExecutionGraph getArchivedExecutionGraph() {
            return archivedExecutionGraph;
        }
    }
}
