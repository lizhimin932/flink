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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.concurrent.FlinkFutureException;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobManagerGateway;

import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.LegacyRestHandler;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterConfiguration;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterConfigurationEntry;
import org.apache.flink.runtime.rest.messages.ClusterConfigurationHeaders;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.util.Preconditions;

import com.fasterxml.jackson.core.JsonGenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Returns the Job Manager's configuration.
 */
public class JobManagerConfigHandler extends AbstractJsonRequestHandler
		implements LegacyRestHandler<DispatcherGateway, ClusterConfiguration, EmptyMessageParameters> {

	private final Configuration config;

	public JobManagerConfigHandler(Executor executor, Configuration config) {
		super(executor);
		this.config = Preconditions.checkNotNull(config);
	}

	@Override
	public String[] getPaths() {
		return new String[]{ClusterConfigurationHeaders.CLUSTER_CONFIG_REST_PATH};
	}

	@Override
	public CompletableFuture<ClusterConfiguration> handleRequest(
			HandlerRequest<EmptyRequestBody, EmptyMessageParameters> request,
			DispatcherGateway gateway) {

		return CompletableFuture.supplyAsync(() -> ClusterConfiguration.from(config), executor);
	}

	@Override
	public CompletableFuture<String> handleJsonRequest(Map<String, String> pathParams, Map<String, String> queryParams, JobManagerGateway jobManagerGateway) {
		return CompletableFuture.supplyAsync(
			() -> {
				try {
					StringWriter writer = new StringWriter();
					JsonGenerator gen = JsonFactory.JACKSON_FACTORY.createGenerator(writer);

					gen.writeStartArray();
					for (String key : config.keySet()) {
						gen.writeStartObject();
						gen.writeStringField(ClusterConfigurationEntry.FIELD_NAME_CONFIG_KEY, key);

						String value = config.getString(key, null);
						// Mask key values which contain sensitive information
						if (value != null && key.toLowerCase().contains("password")) {
							value = "******";
						}
						gen.writeStringField(ClusterConfigurationEntry.FIELD_NAME_CONFIG_VALUE, value);

						gen.writeEndObject();
					}
					gen.writeEndArray();

					gen.close();
					return writer.toString();
				} catch (IOException e) {
					throw new FlinkFutureException("Could not write configuration.", e);
				}
			},
			executor);
	}
}
