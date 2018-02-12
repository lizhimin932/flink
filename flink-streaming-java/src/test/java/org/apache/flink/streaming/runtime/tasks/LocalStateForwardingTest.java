/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProviderImpl;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.TaskLocalStateStore;
import org.apache.flink.runtime.state.TaskLocalStateStoreImpl;
import org.apache.flink.runtime.state.TaskStateManagerImpl;
import org.apache.flink.runtime.state.TestTaskStateManager;
import org.apache.flink.runtime.taskmanager.TestCheckpointResponder;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.mock;

/**
 * Test for forwarding of state reporting to and from {@link org.apache.flink.runtime.state.TaskStateManager}.
 */
public class LocalStateForwardingTest {

	/**
	 * This tests the forwarding of jm and tm-local state from the futures reported by the backends, through the
	 * async checkpointing thread to the {@link org.apache.flink.runtime.state.TaskStateManager}.
	 */
	@Test
	public void testReportingFromSnapshotToTaskStateManager() throws Exception {

		TestTaskStateManager taskStateManager = new TestTaskStateManager();

		StreamMockEnvironment streamMockEnvironment = new StreamMockEnvironment(
			new Configuration(),
			new Configuration(),
			new ExecutionConfig(),
			1024 * 1024,
			new MockInputSplitProvider(),
			0,
			taskStateManager);

		StreamTask testStreamTask = new StreamTaskTest.NoOpStreamTask(streamMockEnvironment);
		CheckpointMetaData checkpointMetaData = new CheckpointMetaData(0L, 0L);
		CheckpointMetrics checkpointMetrics = new CheckpointMetrics();

		Map<OperatorID, OperatorSnapshotFutures> snapshots = new HashMap<>(1);
		OperatorSnapshotFutures osFuture = new OperatorSnapshotFutures();

		osFuture.setKeyedStateManagedFuture(createSnapshotResult(KeyedStateHandle.class));
		osFuture.setKeyedStateRawFuture(createSnapshotResult(KeyedStateHandle.class));
		osFuture.setOperatorStateManagedFuture(createSnapshotResult(OperatorStateHandle.class));
		osFuture.setOperatorStateRawFuture(createSnapshotResult(OperatorStateHandle.class));

		OperatorID operatorID = new OperatorID();
		snapshots.put(operatorID, osFuture);

		StreamTask.AsyncCheckpointRunnable checkpointRunnable =
			new StreamTask.AsyncCheckpointRunnable(
				testStreamTask,
				snapshots,
				checkpointMetaData,
				checkpointMetrics,
				0L);

		checkpointRunnable.run();

		TaskStateSnapshot lastJobManagerTaskStateSnapshot = taskStateManager.getLastJobManagerTaskStateSnapshot();
		TaskStateSnapshot lastTaskManagerTaskStateSnapshot = taskStateManager.getLastTaskManagerTaskStateSnapshot();

		OperatorSubtaskState jmState =
			lastJobManagerTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);

		OperatorSubtaskState tmState =
			lastTaskManagerTaskStateSnapshot.getSubtaskStateByOperatorID(operatorID);

		performCheck(osFuture.getKeyedStateManagedFuture(), jmState.getManagedKeyedState(), tmState.getManagedKeyedState());
		performCheck(osFuture.getKeyedStateRawFuture(), jmState.getRawKeyedState(), tmState.getRawKeyedState());
		performCheck(osFuture.getOperatorStateManagedFuture(), jmState.getManagedOperatorState(), tmState.getManagedOperatorState());
		performCheck(osFuture.getOperatorStateRawFuture(), jmState.getRawOperatorState(), tmState.getRawOperatorState());
	}

	/**
	 * This tests that state that was reported to the {@link org.apache.flink.runtime.state.TaskStateManager} is also
	 * reported to {@link org.apache.flink.runtime.taskmanager.CheckpointResponder} and {@link TaskLocalStateStoreImpl}.
	 */
	@Test
	public void testReportingFromTaskStateManagerToResponderAndTaskLocalStateStore() throws Exception {

		final JobID jobID = new JobID();
		final AllocationID allocationID = new AllocationID();
		final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();
		final CheckpointMetaData checkpointMetaData = new CheckpointMetaData(42L, 4711L);
		final CheckpointMetrics checkpointMetrics = new CheckpointMetrics();
		final int subtaskIdx = 42;
		JobVertexID jobVertexID = new JobVertexID();

		TaskStateSnapshot jm = new TaskStateSnapshot();
		TaskStateSnapshot tm = new TaskStateSnapshot();

		final AtomicBoolean jmReported = new AtomicBoolean(false);
		final AtomicBoolean tmReported = new AtomicBoolean(false);

		TestCheckpointResponder checkpointResponder = new TestCheckpointResponder() {

			@Override
			public void acknowledgeCheckpoint(
				JobID lJobID,
				ExecutionAttemptID lExecutionAttemptID,
				long lCheckpointId,
				CheckpointMetrics lCheckpointMetrics,
				TaskStateSnapshot lSubtaskState) {

				Assert.assertEquals(jobID, lJobID);
				Assert.assertEquals(executionAttemptID, lExecutionAttemptID);
				Assert.assertEquals(checkpointMetaData.getCheckpointId(), lCheckpointId);
				Assert.assertEquals(checkpointMetrics, lCheckpointMetrics);
				jmReported.set(true);
			}
		};

		TemporaryFolder temporaryFolder = new TemporaryFolder();
		temporaryFolder.create();

		Executor executor = Executors.directExecutor();

		LocalRecoveryDirectoryProviderImpl directoryProvider = new LocalRecoveryDirectoryProviderImpl(
			temporaryFolder.newFolder(),
			jobID,
			allocationID,
			jobVertexID,
			subtaskIdx);

		LocalRecoveryConfig localRecoveryConfig = new LocalRecoveryConfig(
			LocalRecoveryConfig.LocalRecoveryMode.ENABLE_FILE_BASED,
			directoryProvider);

		try {
			TaskLocalStateStore taskLocalStateStore =
				new TaskLocalStateStoreImpl(jobID, allocationID, jobVertexID, subtaskIdx, localRecoveryConfig, executor) {
					@Override
					public void storeLocalState(
						@Nonnegative long checkpointId,
						@Nullable TaskStateSnapshot localState) {

						Assert.assertEquals(tm, localState);
						tmReported.set(true);
					}
				};

			TaskStateManagerImpl taskStateManager =
				new TaskStateManagerImpl(
					jobID,
					executionAttemptID,
					taskLocalStateStore,
					null,
					checkpointResponder);

			taskStateManager.reportTaskStateSnapshots(
				checkpointMetaData,
				checkpointMetrics,
				jm,
				tm);
		} catch (Exception ex) {
			temporaryFolder.delete();
			throw new RuntimeException(ex);
		}

		Assert.assertTrue("Reporting for JM state was not called.", jmReported.get());
		Assert.assertTrue("Reporting for TM state was not called.", tmReported.get());
	}

	private static <T extends StateObject> void performCheck(
		Future<SnapshotResult<T>> cc,
		StateObjectCollection<T> jm,
		StateObjectCollection<T> tm) {

		SnapshotResult<T> snapshotResult;
		try {
			snapshotResult = cc.get();
		} catch (Exception e) {
			throw new RuntimeException(e);
		}

		Assert.assertEquals(
			snapshotResult.getJobManagerOwnedSnapshot(),
			jm.iterator().next());

		Assert.assertEquals(
			snapshotResult.getTaskLocalSnapshot(),
			tm.iterator().next());
	}

	private static <T extends StateObject> RunnableFuture<SnapshotResult<T>> createSnapshotResult(Class<T> clazz) {
		return new DoneFuture<>(
			new SnapshotResult<>(mock(clazz), mock(clazz)));
	}
}
