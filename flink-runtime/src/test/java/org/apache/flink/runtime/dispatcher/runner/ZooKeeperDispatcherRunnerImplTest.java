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

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.dispatcher.MemoryArchivedExecutionGraphStore;
import org.apache.flink.runtime.dispatcher.PartialDispatcherServicesWithJobGraphStore;
import org.apache.flink.runtime.dispatcher.SessionDispatcherFactory;
import org.apache.flink.runtime.dispatcher.VoidHistoryServerArchivist;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServicesBuilder;
import org.apache.flink.runtime.highavailability.zookeeper.ZooKeeperRunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.ZooKeeperJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.TestingRpcServiceResource;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.runtime.util.ZooKeeperUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperResource;
import org.apache.flink.util.TestLogger;

import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * Tests for the interaction between the {@link DispatcherRunnerImpl} and ZooKeeper.
 */
public class ZooKeeperDispatcherRunnerImplTest extends TestLogger {

	private static final Logger LOG = LoggerFactory.getLogger(ZooKeeperDispatcherRunnerImplTest.class);

	private static final Time TESTING_TIMEOUT = Time.seconds(10L);

	private static final Duration VERIFICATION_TIMEOUT = Duration.ofSeconds(10L);

	@ClassRule
	public static ZooKeeperResource zooKeeperResource = new ZooKeeperResource();

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@ClassRule
	public static TestingRpcServiceResource testingRpcServiceResource = new TestingRpcServiceResource();

	private BlobServer blobServer;

	private TestingFatalErrorHandler fatalErrorHandler;

	private File clusterHaStorageDir;

	private Configuration configuration;

	@Before
	public void setup() throws IOException {
		fatalErrorHandler = new TestingFatalErrorHandler();
		configuration = new Configuration();
		configuration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zooKeeperResource.getConnectString());
		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.newFolder().getAbsolutePath());

		clusterHaStorageDir = new File(HighAvailabilityServicesUtils.getClusterHighAvailableStoragePath(configuration).toString());
		blobServer = new BlobServer(configuration, BlobUtils.createBlobStoreFromConfig(configuration));
	}

	@After
	public void teardown() throws Exception {
		if (blobServer != null) {
			blobServer.close();
		}

		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
		}
	}

	/**
	 * See FLINK-11665.
	 */
	@Test
	@Ignore
	public void testResourceCleanupUnderLeadershipChange() throws Exception {
		final TestingRpcService rpcService = testingRpcServiceResource.getTestingRpcService();
		final TestingLeaderElectionService dispatcherLeaderElectionService = new TestingLeaderElectionService();
		final SettableLeaderRetrievalService dispatcherLeaderRetriever = new SettableLeaderRetrievalService();

		final CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration);
		try (final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServicesBuilder()
				.setJobGraphStore(ZooKeeperUtils.createJobGraphs(client, configuration))
				.setRunningJobsRegistry(new ZooKeeperRunningJobsRegistry(client, configuration))
				.setDispatcherLeaderElectionService(dispatcherLeaderElectionService)
				.setDispatcherLeaderRetriever(dispatcherLeaderRetriever)
				.setJobMasterLeaderRetrieverFunction(jobId -> ZooKeeperUtils.createLeaderRetrievalService(client, configuration))
				.build()) {

			final PartialDispatcherServicesWithJobGraphStore partialDispatcherServicesWithJobGraphStore = new PartialDispatcherServicesWithJobGraphStore(
				configuration,
				highAvailabilityServices,
				() -> new CompletableFuture<>(),
				blobServer,
				new TestingHeartbeatServices(),
				UnregisteredMetricGroups::createUnregisteredJobManagerMetricGroup,
				new MemoryArchivedExecutionGraphStore(),
				fatalErrorHandler,
				VoidHistoryServerArchivist.INSTANCE,
				null,
				highAvailabilityServices.getJobGraphStore());

			final JobGraph jobGraph = createJobGraphWithBlobs();

			try (final DispatcherRunnerImpl dispatcherRunner = new DispatcherRunnerImpl(SessionDispatcherFactory.INSTANCE, rpcService, partialDispatcherServicesWithJobGraphStore)) {
				// initial run
				DispatcherGateway dispatcherGateway = grantLeadership(dispatcherLeaderElectionService, dispatcherLeaderRetriever, dispatcherRunner);

				LOG.info("Initial job submission {}.", jobGraph.getJobID());
				dispatcherGateway.submitJob(jobGraph, TESTING_TIMEOUT).get();

				dispatcherLeaderElectionService.notLeader();

				// recovering submitted jobs
				LOG.info("Re-grant leadership first time.");
				dispatcherGateway = grantLeadership(dispatcherLeaderElectionService, dispatcherLeaderRetriever, dispatcherRunner);

				LOG.info("Cancel recovered job {}.", jobGraph.getJobID());
				// cancellation of the job should remove everything
				final CompletableFuture<JobResult> jobResultFuture = dispatcherGateway.requestJobResult(jobGraph.getJobID(), TESTING_TIMEOUT);
				dispatcherGateway.cancelJob(jobGraph.getJobID(), TESTING_TIMEOUT).get();

				// a successful cancellation should eventually remove all job information
				final JobResult jobResult = jobResultFuture.get();

				assertThat(jobResult.getApplicationStatus(), is(ApplicationStatus.CANCELED));

				dispatcherLeaderElectionService.notLeader();

				// check that the job has been removed from ZooKeeper
				final ZooKeeperJobGraphStore submittedJobGraphStore = ZooKeeperUtils.createJobGraphs(client, configuration);

				CommonTestUtils.waitUntilCondition(() -> submittedJobGraphStore.getJobIds().isEmpty(), Deadline.fromNow(VERIFICATION_TIMEOUT), 20L);
			}
		}

		// check resource clean up
		assertThat(clusterHaStorageDir.listFiles(), is(emptyArray()));
	}

	private DispatcherGateway grantLeadership(TestingLeaderElectionService dispatcherLeaderElectionService, SettableLeaderRetrievalService dispatcherLeaderRetriever, DispatcherRunnerImpl dispatcherRunner) throws InterruptedException, java.util.concurrent.ExecutionException {
		final UUID leaderSessionId = UUID.randomUUID();
		dispatcherLeaderElectionService.isLeader(leaderSessionId).get();
		// TODO: Remove once runner properly works
		dispatcherLeaderRetriever.notifyListener("foobar", leaderSessionId);

		return dispatcherRunner.getDispatcherGateway().get();
	}

	private JobGraph createJobGraphWithBlobs() throws IOException {
		final JobVertex vertex = new JobVertex("test vertex");
		vertex.setInvokableClass(NoOpInvokable.class);
		vertex.setParallelism(1);

		final JobGraph jobGraph = new JobGraph("Test job graph", vertex);
		jobGraph.setAllowQueuedScheduling(true);

		final PermanentBlobKey permanentBlobKey = blobServer.putPermanent(jobGraph.getJobID(), new byte[256]);
		jobGraph.addUserJarBlobKey(permanentBlobKey);

		return jobGraph;
	}

}
