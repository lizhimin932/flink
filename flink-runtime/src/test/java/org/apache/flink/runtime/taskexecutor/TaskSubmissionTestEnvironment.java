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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.runtime.blob.BlobCacheService;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NetworkEnvironment;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.netty.PartitionProducerStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.TaskExecutorLocalStateStoresManager;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotTable;
import org.apache.flink.runtime.taskexecutor.slot.TimerService;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.NoOpTaskManagerActions;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import java.io.File;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Simple environment setup for task executor task.
 */
public class TaskSubmissionTestEnvironment implements AutoCloseable {
	private final HeartbeatServices heartbeatServices;
	private final TemporaryFolder temporaryFolder;
	private final TestingRpcService testingRpcService;
	private final BlobCacheService blobCacheService;
	private final Time timeout;
	private final JobID jobId;
	private final TestingFatalErrorHandler testingFatalErrorHandler;
	private final TestingHighAvailabilityServices haServices;
	private TimerService<AllocationID> timerService;

	private final boolean mockNetworkEnvironment;
	private final TaskManagerActions taskManagerActions;
	private final TaskSlotTable taskSlotTable;
	private final JobMasterId jobMasterId;
	private final JobMasterGateway jobMasterGateway;
	private final boolean localCommunication;
	private final Configuration configuration;

	private TestingTaskExecutor taskExecutor;

	public TaskSubmissionTestEnvironment(
		JobID jobId,
		JobMasterId jobMasterId,
		int slotSize,
		boolean mockNetworkEnvironment,
		TestingJobMasterGateway jobMasterGateway,
		Configuration configuration,
		boolean localCommunication,
		List<Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>>> taskManagerActionListeners) throws Exception {
		this.heartbeatServices = new HeartbeatServices(1000L, 1000L);
		this.temporaryFolder = new TemporaryFolder();
		this.testingRpcService = new TestingRpcService();
		this.blobCacheService = new BlobCacheService(
			new Configuration(),
			new VoidBlobStore(),
			null);
		this.timeout = Time.milliseconds(10000L);
		this.jobId = jobId;
		this.testingFatalErrorHandler = new TestingFatalErrorHandler();
		this.haServices = new TestingHighAvailabilityServices();

		this.mockNetworkEnvironment = mockNetworkEnvironment;
		this.timerService = new TimerService<>(TestingUtils.defaultExecutor(), timeout.toMilliseconds());
		if (slotSize > 0) {
			this.taskSlotTable = generateTaskSlotTable(slotSize);
		} else {
			this.taskSlotTable = mock(TaskSlotTable.class);
			when(taskSlotTable.tryMarkSlotActive(eq(jobId), any())).thenReturn(true);
			when(taskSlotTable.addTask(any(Task.class))).thenReturn(true);
		}

		if (configuration != null) {
			this.configuration = configuration;
		} else {
			this.configuration = new Configuration();
		}
		this.localCommunication = localCommunication;
		this.haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		this.haServices.setJobMasterLeaderRetriever(jobId, new SettableLeaderRetrievalService());

		if (taskManagerActionListeners.size() == 0) {
			this.taskManagerActions = new NoOpTaskManagerActions();
		} else {
			this.taskManagerActions = new TestTaskManagerActions(taskSlotTable, jobMasterGateway);
			for (Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>> listenerTuple : taskManagerActionListeners) {
				((TestTaskManagerActions) taskManagerActions).addListener(listenerTuple.f0, listenerTuple.f1, listenerTuple.f2);
			}
		}

		if (jobMasterId == null) {
			this.jobMasterId = JobMasterId.generate();
		} else {
			this.jobMasterId = jobMasterId;
		}

		if (jobMasterGateway == null) {
			this.jobMasterGateway = mock(JobMasterGateway.class);
			when(this.jobMasterGateway.getFencingToken()).thenReturn(this.jobMasterId);
		} else {
			this.jobMasterGateway = jobMasterGateway;
		}

		prepareEnvironment();
	}

	public void prepareEnvironment() throws Exception {
		this.temporaryFolder.create();

		final LibraryCacheManager libraryCacheManager = mock(LibraryCacheManager.class);
		when(libraryCacheManager.getClassLoader(any(JobID.class))).thenReturn(ClassLoader.getSystemClassLoader());


		final PartitionProducerStateChecker partitionProducerStateChecker = mock(PartitionProducerStateChecker.class);
		when(partitionProducerStateChecker.requestPartitionProducerState(any(), any(), any()))
			.thenReturn(CompletableFuture.completedFuture(ExecutionState.RUNNING));

		final JobManagerTable jobManagerTable = new JobManagerTable();
		final JobManagerConnection jobManagerConnection =
			new JobManagerConnection(
				jobId,
				ResourceID.generate(),
				jobMasterGateway,
				taskManagerActions,
				mock(CheckpointResponder.class),
				new TestGlobalAggregateManager(),
				libraryCacheManager,
				new RpcResultPartitionConsumableNotifier(jobMasterGateway, testingRpcService.getExecutor(), timeout),
				partitionProducerStateChecker);
		jobManagerTable.put(jobId, jobManagerConnection);

		TaskExecutorLocalStateStoresManager localStateStoresManager = new TaskExecutorLocalStateStoresManager(
			false,
			new File[]{temporaryFolder.newFolder()},
			Executors.directExecutor());

		final ConnectionManager connectionManager;
		if (!localCommunication) {
			NettyConfig nettyConfig = TaskManagerServicesConfiguration
				.fromConfiguration(configuration, InetAddress.getByName(testingRpcService.getAddress()), localCommunication).getNetworkConfig()
				.nettyConfig();
			connectionManager = new NettyConnectionManager(nettyConfig);
		} else {
			connectionManager = new LocalConnectionManager();
		}

		final int numAllBuffers = 10;
		final NetworkEnvironment networkEnvironment;
		if (mockNetworkEnvironment) {
			TaskEventDispatcher taskEventDispatcher = new TaskEventDispatcher();
			networkEnvironment = mock(NetworkEnvironment.class, Mockito.RETURNS_MOCKS);
			when(networkEnvironment.getTaskEventDispatcher()).thenReturn(taskEventDispatcher);
		} else {
			networkEnvironment = createTestNetworkEnvironment(
				numAllBuffers,
				128,
				configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_INITIAL),
				configuration.getInteger(TaskManagerOptions.NETWORK_REQUEST_BACKOFF_MAX),
				2,
				8,
				true,
				connectionManager);
			networkEnvironment.start();
		}

		final TaskManagerServices taskManagerServices = new TaskManagerServicesBuilder()
			.setNetworkEnvironment(networkEnvironment)
			.setTaskSlotTable(taskSlotTable)
			.setJobManagerTable(jobManagerTable)
			.setTaskStateManager(localStateStoresManager)
			.build();

		taskExecutor = createTaskExecutor(taskManagerServices, configuration);

		taskExecutor.start();
		taskExecutor.waitUntilStarted();
	}

	public TestingTaskExecutor getTaskExecutor() {
		return taskExecutor;
	}

	public TaskExecutorGateway getTaskExecutorGateway() {
		return taskExecutor.getSelfGateway(TaskExecutorGateway.class);
	}

	public TaskSlotTable getTaskSlotTable() {
		return taskSlotTable;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterId;
	}

	public TestingFatalErrorHandler getTestingFatalErrorHandler() {
		return testingFatalErrorHandler;
	}

	private TaskSlotTable generateTaskSlotTable(int numSlot) {
		Collection<ResourceProfile> resourceProfiles = new ArrayList<>();
		for (int i = 0; i < numSlot; i++) {
			resourceProfiles.add(ResourceProfile.UNKNOWN);
		}
		return new TaskSlotTable(resourceProfiles, timerService);
	}

	@Nonnull
	private TestingTaskExecutor createTaskExecutor(TaskManagerServices taskManagerServices, Configuration configuration) {
		return new TestingTaskExecutor(
			testingRpcService,
			TaskManagerConfiguration.fromConfiguration(configuration),
			haServices,
			taskManagerServices,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredTaskManagerMetricGroup(),
			null,
			blobCacheService,
			testingFatalErrorHandler
		);
	}

	@Nonnull
	private NetworkEnvironment createTestNetworkEnvironment(
		int numBuffers,
		int memorySegmentSize,
		int partitionRequestInitialBackoff,
		int partitionRequestMaxBackoff,
		int networkBuffersPerChannel,
		int extraNetworkBuffersPerGate,
		boolean enableCreditBased,
		ConnectionManager connectionManager
	) {
		return new NetworkEnvironment(
			new NetworkBufferPool(numBuffers, memorySegmentSize),
			connectionManager,
			new ResultPartitionManager(),
			new TaskEventDispatcher(),
			partitionRequestInitialBackoff,
			partitionRequestMaxBackoff,
			networkBuffersPerChannel,
			extraNetworkBuffersPerGate,
			enableCreditBased);
	}

	@Override
	public void close() throws Exception {
		if (testingRpcService != null) {
			RpcUtils.terminateRpcEndpoint(taskExecutor, timeout);
		}

		if (timerService != null) {
			timerService.stop();
		}

		if (blobCacheService != null) {
			blobCacheService.close();
		}

		temporaryFolder.delete();

		testingFatalErrorHandler.rethrowError();
	}

	public static class TaskSubmissionTestEnvironmentBuilder {
		private JobID jobId;

		private boolean mockNetworkEnvironment = true;
		private int slotSize;
		private JobMasterId jobMasterId;
		private TestingJobMasterGateway jobMasterGateway;
		private boolean localCommunication = true;
		private Configuration configuration;

		private List<Tuple3<ExecutionAttemptID, ExecutionState, CompletableFuture<Void>>> taskManagerActionListeners = new ArrayList<>();
		public TaskSubmissionTestEnvironmentBuilder(JobID jobId) {
			this.jobId = jobId;
		}

		public TaskSubmissionTestEnvironmentBuilder setMockNetworkEnvironment(boolean mockNetworkEnvironment) {
			this.mockNetworkEnvironment = mockNetworkEnvironment;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder setSlotSize(int slotSize) {
			this.slotSize = slotSize;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder setJobMasterId(JobMasterId jobMasterId) {
			this.jobMasterId = jobMasterId;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder setJobMasterGateway(TestingJobMasterGateway jobMasterGateway) {
			this.jobMasterGateway = jobMasterGateway;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder setLocalCommunication(boolean localCommunication) {
			this.localCommunication = localCommunication;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder setConfiguration(Configuration configuration) {
			this.configuration = configuration;
			return this;
		}

		public TaskSubmissionTestEnvironmentBuilder addTaskManagerActionListener(ExecutionAttemptID eid, ExecutionState executionState, CompletableFuture<Void> future) {
			taskManagerActionListeners.add(Tuple3.of(eid, executionState, future));
			return this;
		}

		public TaskSubmissionTestEnvironment build() throws Exception {
			return new TaskSubmissionTestEnvironment(
				jobId,
				jobMasterId,
				slotSize,
				mockNetworkEnvironment,
				jobMasterGateway,
				configuration,
				localCommunication,
				taskManagerActionListeners);
		}
	}
}
