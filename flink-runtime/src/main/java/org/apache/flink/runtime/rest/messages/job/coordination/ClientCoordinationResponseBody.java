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

package org.apache.flink.runtime.rest.messages.job.coordination;

import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.rest.messages.ResponseBody;
import org.apache.flink.runtime.rest.messages.json.SerializedValueDeserializer;
import org.apache.flink.runtime.rest.messages.json.SerializedValueSerializer;
import org.apache.flink.util.SerializedValue;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.annotation.JsonSerialize;

/**
 * Response that carries a serialized {@link CoordinationResponse} to the client.
 */
public class ClientCoordinationResponseBody implements ResponseBody {

	public static final String FIELD_NAME_SERIALIZED_COORDINATION_RESULT = "serializedCoordinationResult";

	@JsonProperty(FIELD_NAME_SERIALIZED_COORDINATION_RESULT)
	@JsonSerialize(using = SerializedValueSerializer.class)
	@JsonDeserialize(using = SerializedValueDeserializer.class)
	private final SerializedValue<CoordinationResponse> serializedCoordinationResponse;

	@JsonCreator
	public ClientCoordinationResponseBody(
		@JsonProperty(FIELD_NAME_SERIALIZED_COORDINATION_RESULT)
			SerializedValue<CoordinationResponse> serializedCoordinationResponse) {
		this.serializedCoordinationResponse = serializedCoordinationResponse;
	}

	@JsonIgnore
	public SerializedValue<CoordinationResponse> getSerializedCoordinationResponse() {
		return serializedCoordinationResponse;
	}
}
