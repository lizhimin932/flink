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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.SimpleTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.time.LocalDate;

@Internal
public final class LocalDateSerializer extends TypeSerializerSingleton<LocalDate> {

	private static final long serialVersionUID = 1L;

	public static final LocalDateSerializer INSTANCE = new LocalDateSerializer();

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public LocalDate createInstance() {
		return LocalDate.of(1970, 1, 1);
	}

	@Override
	public LocalDate copy(LocalDate from) {
		return from;
	}

	@Override
	public LocalDate copy(LocalDate from, LocalDate reuse) {
		return from;
	}

	@Override
	public int getLength() {
		return 6;
	}

	@Override
	public void serialize(LocalDate record, DataOutputView target) throws IOException {
		if (record == null) {
			target.writeInt(Integer.MIN_VALUE);
			target.writeShort(Short.MIN_VALUE);
		} else {
			target.writeInt(record.getYear());
			target.writeByte(record.getMonthValue());
			target.writeByte(record.getDayOfMonth());
		}
	}

	@Override
	public LocalDate deserialize(DataInputView source) throws IOException {
		final int year = source.readInt();
		if (year == Integer.MIN_VALUE) {
			source.readShort();
			return null;
		} else {
			return LocalDate.of(year, source.readByte(), source.readByte());
		}
	}

	@Override
	public LocalDate deserialize(LocalDate reuse, DataInputView source) throws IOException {
		return deserialize(source);
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		target.writeInt(source.readInt());
		target.writeShort(source.readShort());
	}

	@Override
	public TypeSerializerSnapshot<LocalDate> snapshotConfiguration() {
		return new LocalDateSerializerSnapshot();
	}

	// ------------------------------------------------------------------------

	/**
	 * Serializer configuration snapshot for compatibility and format evolution.
	 */
	@SuppressWarnings("WeakerAccess")
	public static final class LocalDateSerializerSnapshot extends SimpleTypeSerializerSnapshot<LocalDate> {

		public LocalDateSerializerSnapshot() {
			super(() -> INSTANCE);
		}
	}
}
