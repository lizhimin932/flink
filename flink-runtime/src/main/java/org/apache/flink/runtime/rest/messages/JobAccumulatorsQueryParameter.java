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

package org.apache.flink.runtime.rest.messages;

/**
 * query parameter for job's accumulator handler {@link org.apache.flink.runtime.rest.handler.job.JobAccumulatorsHandler}.
 */
public class JobAccumulatorsQueryParameter extends MessageQueryParameter<String> {

	private static final String key = "includeSerializedValue";

	public JobAccumulatorsQueryParameter() {
		super(key, MessageParameterRequisiteness.OPTIONAL);
	}

	@Override
	public String convertValueFromString(String value) {
		return value;
	}

	@Override
	public String convertStringToValue(String value) {
		return value;
	}
}
