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

package org.apache.flink.kubernetes.kubeclient.decorators;

import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.kubernetes.kubeclient.parameters.KubernetesJobManagerParameters;
import org.apache.flink.kubernetes.utils.KubernetesUtils;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ServicePort;

import java.io.IOException;
import java.util.List;

/**
 * Creates an internal Service which forwards the requests from the TaskManager(s) to the
 * active JobManager.
 * Note that only the non-HA scenario relies on this Service for internal communication, since
 * in the HA mode, the TaskManager(s) directly connects to the JobManager via IP address.
 */
public class InternalServiceDecorator extends AbstractServiceDecorator {

	public InternalServiceDecorator(KubernetesJobManagerParameters kubernetesJobManagerParameters) {
		super(kubernetesJobManagerParameters);
	}

	@Override
	public List<HasMetadata> buildAccompanyingKubernetesResources() throws IOException {
		final String serviceName = getServiceName();

		// Set job manager address to namespaced service name
		final String namespace = kubernetesJobManagerParameters.getNamespace();
		kubernetesJobManagerParameters.getFlinkConfiguration()
			.setString(JobManagerOptions.ADDRESS, serviceName + "." + namespace);

		return super.buildAccompanyingKubernetesResources();
	}

	@Override
	protected List<ServicePort> getServicePorts() {
		final List<ServicePort> servicePorts = super.getServicePorts();

		servicePorts.add(getServicePort(
			getPortName(JobManagerOptions.PORT.key()),
			kubernetesJobManagerParameters.getRPCPort()));
		servicePorts.add(getServicePort(
			getPortName(BlobServerOptions.PORT.key()),
			kubernetesJobManagerParameters.getBlobServerPort()));

		return servicePorts;
	}

	@Override
	protected String getServiceType() {
		return KubernetesConfigOptions.ServiceExposedType.ClusterIP.name();
	}

	@Override
	protected String getServiceName() {
		return KubernetesUtils.getInternalServiceName(kubernetesJobManagerParameters.getClusterId());
	}
}


