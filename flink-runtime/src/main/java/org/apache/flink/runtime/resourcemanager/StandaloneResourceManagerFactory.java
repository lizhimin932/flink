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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ConfigurationUtils;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.ConfigurationException;

import javax.annotation.Nullable;

/**
 * {@link ResourceManagerFactory} which creates a {@link StandaloneResourceManager}.
 */
public final class StandaloneResourceManagerFactory extends ResourceManagerFactory<ResourceID> {

	private static final StandaloneResourceManagerFactory INSTANCE = new StandaloneResourceManagerFactory();

	private StandaloneResourceManagerFactory() {}

	public static StandaloneResourceManagerFactory getInstance() {
		return INSTANCE;
	}

	@Override
	protected ResourceManager<ResourceID> createResourceManager(
		Configuration configuration,
		ResourceID resourceId,
		RpcService rpcService,
		HighAvailabilityServices highAvailabilityServices,
		HeartbeatServices heartbeatServices,
		FatalErrorHandler fatalErrorHandler,
		ClusterInformation clusterInformation,
		@Nullable String webInterfaceUrl,
		ResourceManagerMetricGroup resourceManagerMetricGroup,
		ResourceManagerRuntimeServices resourceManagerRuntimeServices) {

		final Time standaloneClusterStartupPeriodTime = ConfigurationUtils.getStandaloneClusterStartupPeriodTime(configuration);

		return new StandaloneResourceManager(
			rpcService,
			resourceId,
			highAvailabilityServices,
			heartbeatServices,
			resourceManagerRuntimeServices.getSlotManager(),
			ResourceManagerPartitionTrackerImpl::new,
			resourceManagerRuntimeServices.getJobLeaderIdService(),
			clusterInformation,
			fatalErrorHandler,
			resourceManagerMetricGroup,
			standaloneClusterStartupPeriodTime,
			AkkaUtils.getTimeoutAsTime(configuration));
	}

	@Override
	protected ResourceManagerRuntimeServicesConfiguration createResourceManagerRuntimeServicesConfiguration(
			Configuration configuration) throws ConfigurationException {
		return ResourceManagerRuntimeServicesConfiguration.fromConfiguration(configuration, ArbitraryWorkerResourceSpecFactory.INSTANCE);
	}
}
