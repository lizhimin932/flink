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

@Internal
public final class DoubleSerializer extends TypeSerializerSingleton<Double> {

	private static final long serialVersionUID = 1L;
	
	public static final DoubleSerializer INSTANCE = new DoubleSerializer();
	
	private static final Double ZERO = Double.valueOf(0);

	
	@Override
	public boolean isImmutableType() {
		return true;
	}

	@Override
	public Double createInstance() {
		return ZERO;
	}

	@Override
	public Double copy(Double from) {
		return from;
	}
	
	@Override
	public Double copy(Double from, Double reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Double record, DataOutputView target) throws IOException {
		target.writeDouble(record.doubleValue());
	}

	@Override
	public Double deserialize(DataInputView source) throws IOException {
		return Double.valueOf(source.readDouble());
	}
	
	@Override
	public Double deserialize(Double reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeDouble(source.readDouble());
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof DoubleSerializer;
	}

	// --------------------------------------------------------------------------------------------
	// Serializer configuration snapshotting & reconfiguring
	// --------------------------------------------------------------------------------------------

	@Override
	public PlainSerializationFormatConfigs.DoubleSerializationFormatConfig snapshotConfiguration() {
		return PlainSerializationFormatConfigs.DOUBLE;
	}

	@Override
	public ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot) {
		if (configSnapshot instanceof PlainSerializationFormatConfigs.DoubleSerializationFormatConfig) {
			return ReconfigureResult.COMPATIBLE;
		} else {
			return ReconfigureResult.INCOMPATIBLE;
		}
	}
}
