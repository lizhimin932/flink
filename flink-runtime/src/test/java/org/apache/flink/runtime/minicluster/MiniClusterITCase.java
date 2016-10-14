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

package org.apache.flink.runtime.minicluster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

/**
 * Integration test cases for the {@link MiniCluster}.
 */
public class MiniClusterITCase extends TestLogger {

//	@Test
	public void runJobWithSingleRpcService() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration();

		// should be the default, but set anyways to make sure the test
		// stays valid when the default changes
		cfg.setUseSingleRpcService();

		MiniCluster miniCluster = new MiniCluster(cfg);
		executeJob(miniCluster);
	}

//	@Test
	public void runJobWithMultipleRpcServices() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration();
		cfg.setUseRpcServicePerComponent();

		MiniCluster miniCluster = new MiniCluster(cfg);
		executeJob(miniCluster);
	}

//	@Test
	public void runJobWithMultipleJobManagers() throws Exception {
		MiniClusterConfiguration cfg = new MiniClusterConfiguration();
		cfg.setNumJobManagers(3);

		MiniCluster miniCluster = new MiniCluster(cfg);
		executeJob(miniCluster);
	}

	private static void executeJob(MiniCluster miniCluster) throws Exception {
		miniCluster.start();

		JobGraph job = getSimpleJob();
		miniCluster.runJobBlocking(job);
	}

	private static JobGraph getSimpleJob() {
		JobVertex task = new JobVertex("Test task");
		task.setParallelism(1);
		task.setMaxParallelism(1);
		task.setInvokableClass(NoOpInvokable.class);

		return new JobGraph(new JobID(), "Test Job", task);
	}
}
