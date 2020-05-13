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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.types.logical.TimeType;

/**
 * Converter for {@link TimeType} of {@link Long} external type.
 */
@Internal
class TimeLongConverter implements DataStructureConverter<Integer, Long> {

	private static final long serialVersionUID = 1L;

	@Override
	public Integer toInternal(Long external) {
		return (int) (external / 1000 / 1000);
	}

	@Override
	public Long toExternal(Integer internal) {
		return ((long) internal) * 1000 * 1000;
	}
}
