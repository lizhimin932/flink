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

package org.apache.flink.table.runtime.typeutils.serializers.python;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.base.TypeSerializerSingleton;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.sql.Time;
import java.time.LocalTime;

/**
 * Uses int instead of long as the serialized value. It not only reduces the length of the serialized
 * value, but also makes the serialized value consistent between the legacy planner and the blink
 * planner.
 */
@Internal
public class TimeSerializer extends TypeSerializerSingleton<Time> {

	private static final long serialVersionUID = 1L;

	public static final TimeSerializer INSTANCE = new TimeSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public Time createInstance() {
		return new Time(0L);
	}

	@Override
	public Time copy(Time from) {
		if (from == null) {
			return null;
		}
		return new Time(from.getTime());
	}

	@Override
	public Time copy(Time from, Time reuse) {
		if (from == null) {
			return null;
		}
		reuse.setTime(from.getTime());
		return reuse;
	}

	@Override
	public int getLength() {
		return 8;
	}

	@Override
	public void serialize(Time record, DataOutputView target) throws IOException {
		if (record == null) {
			throw new IllegalArgumentException("The Time record must not be null.");
		}
		target.writeLong(timeToInternal(record));
	}

	@Override
	public Time deserialize(DataInputView source) throws IOException {
		return internalToTime(source.readLong());
	}

	private long timeToInternal(Time time) {
		LocalTime localTime = time.toLocalTime();
		return localTime.toNanoOfDay();
	}

	private Time internalToTime(Long time) {
		return Time.valueOf(LocalTime.ofNanoOfDay(time));
	}

	@Override
	public Time deserialize(Time reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		serialize(deserialize(source), target);
	}

	@Override
	public TypeSerializerSnapshot<Time> snapshotConfiguration() {
		return new TimeSerializerSnapshot();
	}

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class TimeSerializerSnapshot extends SimpleTypeSerializerSnapshot<Time> {

		public TimeSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
