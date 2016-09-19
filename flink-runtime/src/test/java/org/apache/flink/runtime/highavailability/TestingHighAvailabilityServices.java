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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.api.common.JobID;
import org.apache.flink.hadoop.shaded.org.jboss.netty.util.internal.ConcurrentHashMap;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

/**
 * A variant of the HighAvailabilityServices for testing. Each individual service can be set
 * to an arbitrary implementation, such as a mock or default service.
 */
public class TestingHighAvailabilityServices implements HighAvailabilityServices {

	private volatile LeaderRetrievalService resourceManagerLeaderRetriever;

	private ConcurrentHashMap<JobID, LeaderRetrievalService> jobMasterLeaderRetrievers = new ConcurrentHashMap<>();

	private volatile LeaderElectionService jobMasterLeaderElectionService;

	private volatile LeaderElectionService resourceManagerLeaderElectionService;


	// ------------------------------------------------------------------------
	//  Setters for mock / testing implementations
	// ------------------------------------------------------------------------

	public void setResourceManagerLeaderRetriever(LeaderRetrievalService resourceManagerLeaderRetriever) {
		this.resourceManagerLeaderRetriever = resourceManagerLeaderRetriever;
	}

	public void setJobMasterLeaderRetriever(JobID jobID, LeaderRetrievalService jobMasterLeaderRetriever) {
		this.jobMasterLeaderRetrievers.put(jobID, jobMasterLeaderRetriever);
	}

	public void setJobMasterLeaderElectionService(LeaderElectionService leaderElectionService) {
		this.jobMasterLeaderElectionService = leaderElectionService;
	}

	public void setResourceManagerLeaderElectionService(LeaderElectionService leaderElectionService) {
		this.resourceManagerLeaderElectionService = leaderElectionService;
	}

	// ------------------------------------------------------------------------
	//  HA Services Methods
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception {
		LeaderRetrievalService service = this.resourceManagerLeaderRetriever;
		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("ResourceManagerLeaderRetriever has not been set");
		}
	}

	@Override
	public LeaderRetrievalService getJobMasterLeaderRetriever(JobID jobID) throws Exception {
		LeaderRetrievalService service = this.jobMasterLeaderRetrievers.get(jobID);
		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("JobMasterLeaderRetriever has not been set");
		}
	}

	@Override
	public LeaderElectionService getJobMasterLeaderElectionService(JobID jobID) throws Exception {
		LeaderElectionService service = jobMasterLeaderElectionService;

		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("JobMasterLeaderElectionService has not been set");
		}
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() throws Exception {
		LeaderElectionService service = resourceManagerLeaderElectionService;

		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("ResourceManagerLeaderElectionService has not been set");
		}
	}
}
