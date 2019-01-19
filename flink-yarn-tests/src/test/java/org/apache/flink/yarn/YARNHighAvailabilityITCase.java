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

package org.apache.flink.yarn;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.deployment.ClusterDeploymentException;
import org.apache.flink.client.deployment.ClusterSpecification;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.rest.messages.job.JobDetailsInfo;
import org.apache.flink.util.OperatingSystem;
import org.apache.flink.util.function.SupplierWithException;
import org.apache.flink.yarn.configuration.YarnConfigOptions;
import org.apache.flink.yarn.entrypoint.YarnSessionClusterEntrypoint;
import org.apache.flink.yarn.testjob.YarnTestJob;
import org.apache.flink.yarn.util.YarnTestUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.protocolrecords.StopContainersRequest;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.NMTokenIdentifier;
import org.apache.hadoop.yarn.server.nodemanager.NodeManager;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.container.Container;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.flink.util.Preconditions.checkState;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assume.assumeTrue;

/**
 * Tests that verify correct HA behavior.
 */
public class YARNHighAvailabilityITCase extends YarnTestBase {

	@ClassRule
	public static final TemporaryFolder FOLDER = new TemporaryFolder();

	private static final String LOG_DIR = "flink-yarn-tests-ha";
	private static final Duration TIMEOUT = Duration.ofSeconds(200L);

	private static TestingServer zkServer;
	private static String storageDir;

	@BeforeClass
	public static void setup() throws Exception {
		zkServer = new TestingServer();

		storageDir = FOLDER.newFolder().getAbsolutePath();

		// startYARNWithConfig should be implemented by subclass
		YARN_CONFIGURATION.setClass(YarnConfiguration.RM_SCHEDULER, CapacityScheduler.class, ResourceScheduler.class);
		YARN_CONFIGURATION.set(YarnTestBase.TEST_CLUSTER_NAME_KEY, LOG_DIR);
		YARN_CONFIGURATION.setInt(YarnConfiguration.NM_PMEM_MB, 4096);
		startYARNWithConfig(YARN_CONFIGURATION);
	}

	@AfterClass
	public static void teardown() throws Exception {
		if (zkServer != null) {
			zkServer.stop();
			zkServer = null;
		}
	}

	/**
	 * Tests that Yarn will restart a killed {@link YarnSessionClusterEntrypoint} which will then resume
	 * a persisted {@link JobGraph}.
	 */
	@Test
	public void testKillYarnSessionClusterEntrypoint() throws Exception {
		assumeTrue(
			"This test kills processes via the pkill command. Thus, it only runs on Linux, Mac OS, Free BSD and Solaris.",
			OperatingSystem.isLinux() || OperatingSystem.isMac() || OperatingSystem.isFreeBSD() || OperatingSystem.isSolaris());

		final YarnClusterDescriptor yarnClusterDescriptor = setupYarnClusterDescriptor();

		final RestClusterClient<ApplicationId> restClusterClient = deploySessionCluster(yarnClusterDescriptor);

		final JobGraph job = createJobGraph();

		final JobID jobId = submitJob(restClusterClient, job);

		final ApplicationId id = restClusterClient.getClusterId();

		waitUntilJobIsRunning(restClusterClient, jobId);

		killApplicationMaster(yarnClusterDescriptor.getYarnSessionClusterEntrypoint());
		waitForApplicationAttempt(id, 2);

		waitUntilJobIsRunning(restClusterClient, jobId);

		killApplicationAndWait(id);
	}

	@Test
	public void testJobRecoversAfterKillingTaskManager() throws Exception {
		final RestClusterClient<ApplicationId> restClusterClient = deploySessionCluster(setupYarnClusterDescriptor());
		final JobID jobId = submitJob(restClusterClient, createJobGraph());
		final ApplicationId id = restClusterClient.getClusterId();
		waitUntilJobIsRunning(restClusterClient, jobId);

		stopTaskManagerContainer();
		waitUntilJobIsRunning(restClusterClient, jobId);

		killApplicationAndWait(id);
	}

	private void waitForApplicationAttempt(final ApplicationId applicationId, final int attemptId) throws Exception {
		final YarnClient yarnClient = getYarnClient();
		checkState(yarnClient != null, "yarnClient must be initialized");

		waitUntilCondition(() -> {
			final ApplicationReport applicationReport = yarnClient.getApplicationReport(applicationId);
			return applicationReport.getCurrentApplicationAttemptId().getAttemptId() >= attemptId;
		}, Deadline.fromNow(TIMEOUT));
	}

	/**
	 * Stops a container running {@link YarnTaskExecutorRunner}.
	 */
	private void stopTaskManagerContainer() throws Exception {
		// find container id of taskManager:
		ContainerId taskManagerContainer = null;
		NodeManager nodeManager = null;
		NMTokenIdentifier nmIdent = null;
		UserGroupInformation remoteUgi = UserGroupInformation.getCurrentUser();

		for (int nmId = 0; nmId < NUM_NODEMANAGERS; nmId++) {
			NodeManager nm = yarnCluster.getNodeManager(nmId);
			ConcurrentMap<ContainerId, Container> containers = nm.getNMContext().getContainers();
			for (Map.Entry<ContainerId, Container> entry : containers.entrySet()) {
				String command = StringUtils.join(entry.getValue().getLaunchContext().getCommands(), " ");
				if (command.contains(YarnTaskExecutorRunner.class.getSimpleName())) {
					taskManagerContainer = entry.getKey();
					nodeManager = nm;
					nmIdent = new NMTokenIdentifier(taskManagerContainer.getApplicationAttemptId(), null, "", 0);
					// allow myself to do stuff with the container
					// remoteUgi.addCredentials(entry.getValue().getCredentials());
					remoteUgi.addTokenIdentifier(nmIdent);
				}
			}
			sleep(500);
		}

		assertNotNull("Unable to find container with TaskManager", taskManagerContainer);
		assertNotNull("Illegal state", nodeManager);

		List<ContainerId> toStop = new LinkedList<>();
		toStop.add(taskManagerContainer);
		StopContainersRequest scr = StopContainersRequest.newInstance(toStop);

		nodeManager.getNMContext().getContainerManager().stopContainers(scr);

		// cleanup auth for the subsequent tests.
		remoteUgi.getTokenIdentifiers().remove(nmIdent);
	}

	private void killApplicationAndWait(final ApplicationId id) throws Exception {
		final YarnClient yarnClient = getYarnClient();
		checkState(yarnClient != null, "yarnClient must be initialized");

		yarnClient.killApplication(id);

		waitUntilCondition(() -> !yarnClient.getApplications(EnumSet.of(YarnApplicationState.KILLED, YarnApplicationState.FINISHED)).isEmpty(),
			Deadline.fromNow(TIMEOUT));
	}

	@Nonnull
	private YarnClusterDescriptor setupYarnClusterDescriptor() {
		final Configuration flinkConfiguration = new Configuration();
		flinkConfiguration.setString(YarnConfigOptions.APPLICATION_ATTEMPTS, "10");
		flinkConfiguration.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		flinkConfiguration.setString(HighAvailabilityOptions.HA_STORAGE_PATH, storageDir);
		flinkConfiguration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, zkServer.getConnectString());
		flinkConfiguration.setInteger(HighAvailabilityOptions.ZOOKEEPER_SESSION_TIMEOUT, 1000);

		flinkConfiguration.setString(ConfigConstants.RESTART_STRATEGY, "fixed-delay");
		flinkConfiguration.setInteger(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);

		final int minMemory = 100;
		flinkConfiguration.setInteger(ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN, minMemory);

		return createYarnClusterDescriptor(flinkConfiguration);
	}

	private RestClusterClient<ApplicationId> deploySessionCluster(YarnClusterDescriptor yarnClusterDescriptor) throws ClusterDeploymentException {
		final int containerMemory = 256;
		final ClusterClient<ApplicationId> yarnClusterClient = yarnClusterDescriptor.deploySessionCluster(
			new ClusterSpecification.ClusterSpecificationBuilder()
				.setMasterMemoryMB(containerMemory)
				.setTaskManagerMemoryMB(containerMemory)
				.setSlotsPerTaskManager(1)
				.createClusterSpecification());

		assertThat(yarnClusterClient, is(instanceOf(RestClusterClient.class)));
		return (RestClusterClient<ApplicationId>) yarnClusterClient;
	}

	private JobID submitJob(RestClusterClient<ApplicationId> restClusterClient, JobGraph job) throws InterruptedException, java.util.concurrent.ExecutionException {
		final CompletableFuture<JobSubmissionResult> jobSubmissionResultCompletableFuture = restClusterClient.submitJob(job);

		final JobSubmissionResult jobSubmissionResult = jobSubmissionResultCompletableFuture.get();
		return jobSubmissionResult.getJobID();
	}

	private void killApplicationMaster(final String processName) throws IOException, InterruptedException {
		final Process exec = Runtime.getRuntime().exec("pkill -f " + processName);
		assertThat(exec.waitFor(), is(0));
	}

	@Nonnull
	private JobGraph createJobGraph() {
		final JobGraph job = YarnTestJob.createJob();
		final File testingJar =
			YarnTestBase.findFile("..", new YarnTestUtils.TestJarFinder("flink-yarn-tests"));

		assertThat(testingJar, notNullValue());

		job.addJar(new org.apache.flink.core.fs.Path(testingJar.toURI()));
		return job;
	}

	private void waitUntilJobIsRunning(RestClusterClient<ApplicationId> restClusterClient, JobID jobId) throws Exception {
		waitUntilCondition(
			() -> {
				final JobDetailsInfo jobDetails = restClusterClient.getJobDetails(jobId).get();
				return jobDetails.getJobVertexInfos()
					.stream()
					.map(toExecutionState())
					.allMatch(isRunning());
			},
			Deadline.fromNow(TIMEOUT));
	}

	private static Function<JobDetailsInfo.JobVertexDetailsInfo, ExecutionState> toExecutionState() {
		return JobDetailsInfo.JobVertexDetailsInfo::getExecutionState;
	}

	private static Predicate<ExecutionState> isRunning() {
		return executionState -> executionState == ExecutionState.RUNNING;
	}

}
