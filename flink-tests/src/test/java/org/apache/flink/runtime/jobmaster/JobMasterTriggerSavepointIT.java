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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.program.MiniClusterClient;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointTriggerException;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.ExceptionUtils;

import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.isOneOf;

/**
 * Tests for {@link org.apache.flink.runtime.jobmaster.JobMaster#triggerSavepoint(String, boolean, Time)}.
 *
 * @see org.apache.flink.runtime.jobmaster.JobMaster
 */
@Category(Flip6.class)
public class JobMasterTriggerSavepointIT extends AbstractTestBase {

	private static CountDownLatch invokeLatch;

	private static volatile CountDownLatch triggerCheckpointLatch;

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	private Path savepointDirectory;
	private MiniClusterClient clusterClient;
	private JobGraph jobGraph;

	@Before
	public void setUp() throws Exception {
		invokeLatch = new CountDownLatch(1);
		triggerCheckpointLatch = new CountDownLatch(1);
		savepointDirectory = temporaryFolder.newFolder().toPath();

		Assume.assumeTrue(
			"ClusterClient is not an instance of MiniClusterClient",
			miniClusterResource.getClusterClient() instanceof MiniClusterClient);

		clusterClient = (MiniClusterClient) miniClusterResource.getClusterClient();
		clusterClient.setDetached(true);

		jobGraph = new JobGraph();

		final JobVertex vertex = new JobVertex("testVertex");
		vertex.setInvokableClass(NoOpBlockingInvokable.class);
		jobGraph.addVertex(vertex);

		jobGraph.setSnapshotSettings(new JobCheckpointingSettings(
			Collections.singletonList(vertex.getID()),
			Collections.singletonList(vertex.getID()),
			Collections.singletonList(vertex.getID()),
			new CheckpointCoordinatorConfiguration(
				10,
				60_000,
				10,
				1,
				CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
				true),
			null
		));

		clusterClient.submitJob(jobGraph, ClassLoader.getSystemClassLoader());
		invokeLatch.await(60, TimeUnit.SECONDS);
		waitForJob();
	}

	@Test
	public void testStopJobAfterSavepoint() throws Exception {
		final String savepointLocation = cancelWithSavepoint();
		final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);

		assertThat(jobStatus, isOneOf(JobStatus.CANCELED, JobStatus.CANCELLING));

		final List<Path> savepoints = Files.list(savepointDirectory).map(Path::getFileName).collect(Collectors.toList());
		assertThat(savepoints, hasItem(Paths.get(savepointLocation).getFileName()));
	}

	@Test
	public void testDoNotCancelJobIfSavepointFails() throws Exception {
		try {
			Files.setPosixFilePermissions(savepointDirectory, Collections.emptySet());
		} catch (IOException e) {
			Assume.assumeNoException(e);
		}

		try {
			cancelWithSavepoint();
		} catch (Exception e) {
			assertThat(ExceptionUtils.findThrowable(e, CheckpointTriggerException.class).isPresent(), equalTo(true));
		}

		final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
		assertThat(jobStatus, equalTo(JobStatus.RUNNING));

		// assert that checkpoints are continued to be triggered
		triggerCheckpointLatch = new CountDownLatch(1);
		assertThat(triggerCheckpointLatch.await(60, TimeUnit.SECONDS), equalTo(true));
	}

	private void waitForJob() throws Exception {
		for (int i = 0; i < 60; i++) {
			try {
				final JobStatus jobStatus = clusterClient.getJobStatus(jobGraph.getJobID()).get(60, TimeUnit.SECONDS);
				assertThat(jobStatus.isGloballyTerminalState(), equalTo(false));
				if (jobStatus == JobStatus.RUNNING) {
					return;
				}
			} catch (ExecutionException ignored) {
				// JobManagerRunner is not yet registered in Dispatcher
			}
			Thread.sleep(1000);
		}
		throw new AssertionError("Job did not become running within timeout.");
	}

	/**
	 * Invokable which calls {@link CountDownLatch#countDown()} on
	 * {@link JobMasterTriggerSavepointIT#invokeLatch}, and then blocks afterwards.
	 */
	public static class NoOpBlockingInvokable extends AbstractInvokable {

		public NoOpBlockingInvokable(final Environment environment) {
			super(environment);
		}

		@Override
		public void invoke() {
			invokeLatch.countDown();
			try {
				Thread.sleep(Long.MAX_VALUE);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		}

		@Override
		public boolean triggerCheckpoint(final CheckpointMetaData checkpointMetaData, final CheckpointOptions checkpointOptions) throws Exception {
			final TaskStateSnapshot checkpointStateHandles = new TaskStateSnapshot();
			checkpointStateHandles.putSubtaskStateByOperatorID(
				OperatorID.fromJobVertexID(getEnvironment().getJobVertexId()),
				new OperatorSubtaskState());

			getEnvironment().acknowledgeCheckpoint(
				checkpointMetaData.getCheckpointId(),
				new CheckpointMetrics(),
				checkpointStateHandles);

			triggerCheckpointLatch.countDown();

			return true;
		}

		@Override
		public void notifyCheckpointComplete(final long checkpointId) throws Exception {
		}
	}

	private String cancelWithSavepoint() throws Exception {
		return clusterClient.cancelWithSavepoint(
			jobGraph.getJobID(),
			savepointDirectory.toAbsolutePath().toString());
	}

}
