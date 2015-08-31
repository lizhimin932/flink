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

package org.apache.flink.runtime.leaderelection;

import org.apache.curator.test.TestingCluster;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobmanager.JobManager;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.util.LeaderElectionUtils;
import org.apache.flink.runtime.util.LeaderRetrievalUtils;
import org.apache.flink.util.TestLogger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import scala.Option;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class ZooKeeperLeaderRetrievalTest extends TestLogger{

	private TestingCluster testingCluster;

	@Before
	public void before() {
		testingCluster = new TestingCluster(3);
		try {
			testingCluster.start();
		} catch (Exception e) {
			throw new RuntimeException("Could not start ZooKeeper testing cluster.", e);
		}
	}

	@After
	public void after() {
		if(testingCluster != null) {
			try {
				testingCluster.stop();
			} catch (IOException e) {
				throw new RuntimeException("Could not stop ZooKeeper testing cluster.", e);
			}
			testingCluster = null;
		}
	}

	/**
	 * Tests that LeaderRetrievalUtils.findConnectingAdress finds the correct connecting address
	 * in case of an old leader address in ZooKeeper and a subsequent election of a new leader.
	 * The findConnectingAddress should block until the new leader has been elected and his
	 * address has been written to ZooKeeper.
	 */
	@Test
	public void testConnectingAddressRetrievalWithDelayedLeaderElection() throws Exception {
		FiniteDuration timeout = new FiniteDuration(1, TimeUnit.MINUTES);
		Configuration config = new Configuration();

		long sleepingTime = 1000;

		config.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		config.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());

		LeaderElectionService leaderElectionService = null;
		LeaderElectionService faultyLeaderElectionService;

		ServerSocket serverSocket;
		InetAddress localHost;

		Thread thread;

		try {

			InetSocketAddress wrongInetSocketAddress = new InetSocketAddress(InetAddress.getByName("1.1.1.1"), 1234);

			String wrongAddress = JobManager.getRemoteJobManagerAkkaURL(wrongInetSocketAddress, Option.<String>empty());

			try {
				localHost = InetAddress.getLocalHost();
				serverSocket = new ServerSocket(0, 50, localHost);
			} catch (UnknownHostException e) {
				// may happen if disconnected. skip test.
				System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
				return;
			}
			catch (IOException e) {
				// may happen in certain test setups, skip test.
				System.err.println("Skipping 'testNetworkInterfaceSelection' test.");
				return;
			}

			InetSocketAddress correctInetSocketAddress = new InetSocketAddress(localHost, serverSocket.getLocalPort());

			String correctAddress = JobManager.getRemoteJobManagerAkkaURL(correctInetSocketAddress, Option.<String>empty());

			faultyLeaderElectionService = LeaderElectionUtils.createLeaderElectionService(config);
			TestingContender wrongLeaderAddressContender = new TestingContender(wrongAddress, faultyLeaderElectionService);

			faultyLeaderElectionService.start(wrongLeaderAddressContender);

			FindConnectingAddress findConnectingAddress = new FindConnectingAddress(config, timeout);

			thread = new Thread(findConnectingAddress);

			thread.start();

			leaderElectionService = LeaderElectionUtils.createLeaderElectionService(config);
			TestingContender correctLeaderAddressContender = new TestingContender(correctAddress, leaderElectionService);

			Thread.sleep(sleepingTime);

			faultyLeaderElectionService.stop();

			leaderElectionService.start(correctLeaderAddressContender);

			thread.join();

			InetAddress result = findConnectingAddress.getInetAddress();

			// check that we can connect to the localHost
			Socket socket = new Socket();
			try {
				// port 0 = let the OS choose the port
				SocketAddress bindP = new InetSocketAddress(result, 0);
				// machine
				socket.bind(bindP);
				socket.connect(correctInetSocketAddress, 1000);
			} finally {
				socket.close();
			}
		} finally {
			if (leaderElectionService != null) {
				leaderElectionService.stop();
			}
		}
	}

	/**
	 * Tests that the LeaderRetrievalUtils.findConnectingAddress stops trying to find the
	 * connecting address if no leader address has been specified. The call should return
	 * then InetAddress.getLocalHost().
	 */
	@Test
	public void testTimeoutOfFindConnectingAddress() throws Exception {
		Configuration config = new Configuration();
		config.setString(ConfigConstants.RECOVERY_MODE, "zookeeper");
		config.setString(ConfigConstants.ZOOKEEPER_QUORUM_KEY, testingCluster.getConnectString());

		FiniteDuration timeout = new FiniteDuration(10, TimeUnit.SECONDS);

		LeaderRetrievalService leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(config);
		InetAddress result = LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, timeout);

		assertEquals(InetAddress.getLocalHost(), result);
	}

	class FindConnectingAddress implements Runnable {

		private final Configuration config;
		private final FiniteDuration timeout;

		private InetAddress result;
		private Exception exception;

		public FindConnectingAddress(Configuration config, FiniteDuration timeout) {
			this.config = config;
			this.timeout = timeout;
		}

		@Override
		public void run() {
			try {
				LeaderRetrievalService leaderRetrievalService = LeaderRetrievalUtils.createLeaderRetrievalService(config);
				result = LeaderRetrievalUtils.findConnectingAddress(leaderRetrievalService, timeout);
			} catch (Exception e) {
				exception = e;
			}
		}

		public InetAddress getInetAddress() throws Exception {
			if (exception != null) {
				throw exception;
			} else {
				return result;
			}
		}
	}
}
