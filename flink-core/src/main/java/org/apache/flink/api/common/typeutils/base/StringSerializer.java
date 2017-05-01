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

package org.apache.flink.api.common.typeutils.base;

import java.io.IOException;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.ReconfigureResult;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.types.StringValue;

@Internal
public final class StringSerializer extends TypeSerializerSingleton<String> {

	private static final long serialVersionUID = 1L;
	
	public static final StringSerializer INSTANCE = new StringSerializer();
	
	private static final String EMPTY = "";

	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public String createInstance() {
		return EMPTY;
	}

	@Override
	public String copy(String from) {
		return from;
	}
	
	@Override
	public String copy(String from, String reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public void serialize(String record, DataOutputView target) throws IOException {
		StringValue.writeString(record, target);
	}

	@Override
	public String deserialize(DataInputView source) throws IOException {
		return StringValue.readString(source);
	}
	
	@Override
	public String deserialize(String record, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		StringValue.copyString(source, target);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof StringSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & reconfiguring
	// --------------------------------------------------------------------------------------------

	@Override
	public PlainSerializationFormatConfigs.StringSerializationFormatConfig snapshotConfiguration() {
		return PlainSerializationFormatConfigs.STRING;
	}

	@Override
	public ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof PlainSerializationFormatConfigs.StringSerializationFormatConfig) {
			return ReconfigureResult.COMPATIBLE;
		} else {
			return ReconfigureResult.INCOMPATIBLE;
		}
	}
}
