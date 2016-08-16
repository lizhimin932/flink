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

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} for the non-high-availability case.
 * This implementation can be used for testing, and for cluster setups that do not
 * tolerate failures of the master processes (JobManager, ResourceManager).
 * 
 * <p>This implementation has no dependencies on any external services. It returns fix
 * pre-configured leaders, and stores checkpoints and metadata simply on the heap and therefore
 * in volatile memory.
 */
public class NonHaServices implements HighAvailabilityServices {

	/** The fix address of the ResourceManager */
	private final String resourceManagerAddress;

	/**
	 * Creates a new services class for the fix pre-defined leaders.
	 * 
	 * @param resourceManagerAddress    The fix address of the ResourceManager
	 */
	public NonHaServices(String resourceManagerAddress) {
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress);
	}

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception {
		return new StandaloneLeaderRetrievalService(resourceManagerAddress, new UUID(0, 0));
	}
}
