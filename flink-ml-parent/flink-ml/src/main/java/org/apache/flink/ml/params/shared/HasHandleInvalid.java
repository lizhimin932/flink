/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params.shared;

import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.params.ParamInfo;

/**
 * The handle method of invalid value. include： error, skip, optimistic.
 */
public interface HasHandleInvalid<T> extends BaseWithParam <T> {
	ParamInfo <String> HANDLE_INVALID = new ParamInfo <>(
		"handleInvalidMethod",
		"The handle method of invalid value. include： error, skip, optimistic",
		true, "error",
		String.class
	);

	default String getHandleInvalid() {
		return getParams().get(HANDLE_INVALID);
	}

	default T setHandleInvalid(String value) {
		return set(HANDLE_INVALID, value);
	}

}
