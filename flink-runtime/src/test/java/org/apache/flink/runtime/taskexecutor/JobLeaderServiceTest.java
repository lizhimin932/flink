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
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public class JobLeaderServiceTest {
	@Test
	public void testAddJob() throws Exception {
		JobLeaderService jobLeaderService = new JobLeaderService(mock(TaskManagerLocation.class));

		HighAvailabilityServices highAvailabilityServices = mock(HighAvailabilityServices.class);
		LeaderRetrievalService leaderRetrievalService = mock(LeaderRetrievalService.class);

		when(highAvailabilityServices.getJobManagerLeaderRetriever(any(JobID.class)))
			.thenReturn(leaderRetrievalService);

		jobLeaderService.start("localhost", mock(RpcService.class), highAvailabilityServices,
			mock(JobLeaderListener.class));

		JobID jobID = JobID.generate();

		jobLeaderService.addJob(jobID, "targetAddress");

		assertTrue(jobLeaderService.containsJob(jobID));

		// Invoke addJob multiple times
		jobLeaderService.addJob(jobID, "targetAddress");

		assertTrue(jobLeaderService.containsJob(jobID));
		// Just start one leaderRetrievalService
		verify(leaderRetrievalService, times(1)).start(any(LeaderRetrievalListener.class));
	}
}
