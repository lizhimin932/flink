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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.task.AbstractEvent;
import org.apache.flink.runtime.event.task.TaskEvent;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class EventSerializerTest {

	@Test
	public void testSerializeDeserializeEvent() {

		TestTaskEvent expected = new TestTaskEvent(Math.random(), 12361231273L);

		ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(expected);

		AbstractEvent actual = EventSerializer.fromSerializedEvent(serializedEvent, getClass().getClassLoader());

		assertEquals(expected, actual);
	}

	public static class TestTaskEvent extends TaskEvent {

		private double val0;

		private long val1;

		public TestTaskEvent() {
		}

		public TestTaskEvent(double val0, long val1) {
			this.val0 = val0;
			this.val1 = val1;
		}

		@Override
		public void write(DataOutputView out) throws IOException {
			out.writeDouble(val0);
			out.writeLong(val1);
		}

		@Override
		public void read(DataInputView in) throws IOException {
			val0 = in.readDouble();
			val1 = in.readLong();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof TestTaskEvent) {
				TestTaskEvent other = (TestTaskEvent) obj;

				return val0 == other.val0 && val1 == other.val1;
			}

			return false;
		}
	}
}
