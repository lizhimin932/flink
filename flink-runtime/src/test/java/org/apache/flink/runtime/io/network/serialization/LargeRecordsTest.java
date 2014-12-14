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

package org.apache.flink.runtime.io.network.serialization;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.Buffer;
import org.apache.flink.runtime.io.network.serialization.types.IntType;
import org.apache.flink.runtime.io.network.serialization.types.LargeObjectType;
import org.apache.flink.runtime.io.network.serialization.types.SerializationTestType;
import org.junit.Test;

public class LargeRecordsTest {

	@Test
	public void testHandleMixedLargeRecords() {
		try {
			final int NUM_RECORDS = 99;
			final int SEGMENT_SIZE = 32 * 1024;

			final RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();
			final RecordDeserializer<SerializationTestType> deserializer = new AdaptiveSpanningRecordDeserializer<SerializationTestType>();

			final Buffer buffer = new Buffer(new MemorySegment(new byte[SEGMENT_SIZE]), SEGMENT_SIZE, null);

			List<SerializationTestType> originalRecords = new ArrayList<SerializationTestType>();
			List<SerializationTestType> deserializedRecords = new ArrayList<SerializationTestType>();
			
			LargeObjectType genLarge = new LargeObjectType();
			
			Random rnd = new Random();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				if (i % 2 == 0) {
					originalRecords.add(new IntType(42));
					deserializedRecords.add(new IntType());
				} else {
					originalRecords.add(genLarge.getRandom(rnd));
					deserializedRecords.add(new LargeObjectType());
				}
			}

			// -------------------------------------------------------------------------------------------------------------

			serializer.setNextBuffer(buffer);
			
			int numRecordsDeserialized = 0;
			
			for (SerializationTestType record : originalRecords) {

				// serialize record
				if (serializer.addRecord(record).isFullBuffer()) {

					// buffer is full => move to deserializer
					deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), SEGMENT_SIZE);

					// deserialize records, as many complete as there are
					while (numRecordsDeserialized < deserializedRecords.size()) {
						SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
					
						if (deserializer.getNextRecord(next).isFullRecord()) {
							assertEquals(originalRecords.get(numRecordsDeserialized), next);
							numRecordsDeserialized++;
						} else {
							break;
						}
					}

					// move buffers as long as necessary (for long records)
					while (serializer.setNextBuffer(buffer).isFullBuffer()) {
						deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), SEGMENT_SIZE);
					}
					
					// deserialize records, as many as there are in the last buffer
					while (numRecordsDeserialized < deserializedRecords.size()) {
						SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
					
						if (deserializer.getNextRecord(next).isFullRecord()) {
							assertEquals(originalRecords.get(numRecordsDeserialized), next);
							numRecordsDeserialized++;
						} else {
							break;
						}
					}
				}
			}
			
			// move the last (incomplete buffer)
			Buffer last = serializer.getCurrentBuffer();
			deserializer.setNextMemorySegment(last.getMemorySegment(), last.size());
			serializer.clear();
			
			// deserialize records, as many as there are in the last buffer
			while (numRecordsDeserialized < deserializedRecords.size()) {
				SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
			
				assertTrue(deserializer.getNextRecord(next).isFullRecord());
				assertEquals(originalRecords.get(numRecordsDeserialized), next);
				numRecordsDeserialized++;
			}
			
			// might be that the last big records has not yet been fully moved, and a small one is missing
			assertFalse(serializer.hasData());
			assertFalse(deserializer.hasUnfinishedData());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testHandleMixedLargeRecordsSpillingAdaptiveSerializer() {
		try {
			final int NUM_RECORDS = 99;
			final int SEGMENT_SIZE = 32 * 1024;

			final RecordSerializer<SerializationTestType> serializer = new SpanningRecordSerializer<SerializationTestType>();
			final RecordDeserializer<SerializationTestType> deserializer = new SpillingAdaptiveSpanningRecordDeserializer<SerializationTestType>();

			final Buffer buffer = new Buffer(new MemorySegment(new byte[SEGMENT_SIZE]), SEGMENT_SIZE, null);

			List<SerializationTestType> originalRecords = new ArrayList<SerializationTestType>();
			List<SerializationTestType> deserializedRecords = new ArrayList<SerializationTestType>();
			
			LargeObjectType genLarge = new LargeObjectType();
			
			Random rnd = new Random();
			
			for (int i = 0; i < NUM_RECORDS; i++) {
				if (i % 2 == 0) {
					originalRecords.add(new IntType(42));
					deserializedRecords.add(new IntType());
				} else {
					originalRecords.add(genLarge.getRandom(rnd));
					deserializedRecords.add(new LargeObjectType());
				}
			}

			// -------------------------------------------------------------------------------------------------------------

			serializer.setNextBuffer(buffer);
			
			int numRecordsDeserialized = 0;
			
			for (SerializationTestType record : originalRecords) {

				// serialize record
				if (serializer.addRecord(record).isFullBuffer()) {

					// buffer is full => move to deserializer
					deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), SEGMENT_SIZE);

					// deserialize records, as many complete as there are
					while (numRecordsDeserialized < deserializedRecords.size()) {
						SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
					
						if (deserializer.getNextRecord(next).isFullRecord()) {
							assertEquals(originalRecords.get(numRecordsDeserialized), next);
							numRecordsDeserialized++;
						} else {
							break;
						}
					}

					// move buffers as long as necessary (for long records)
					while (serializer.setNextBuffer(buffer).isFullBuffer()) {
						deserializer.setNextMemorySegment(serializer.getCurrentBuffer().getMemorySegment(), SEGMENT_SIZE);
					}
					
					// deserialize records, as many as there are in the last buffer
					while (numRecordsDeserialized < deserializedRecords.size()) {
						SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
					
						if (deserializer.getNextRecord(next).isFullRecord()) {
							assertEquals(originalRecords.get(numRecordsDeserialized), next);
							numRecordsDeserialized++;
						} else {
							break;
						}
					}
				}
			}
			
			// move the last (incomplete buffer)
			Buffer last = serializer.getCurrentBuffer();
			deserializer.setNextMemorySegment(last.getMemorySegment(), last.size());
			serializer.clear();
			
			// deserialize records, as many as there are in the last buffer
			while (numRecordsDeserialized < deserializedRecords.size()) {
				SerializationTestType next = deserializedRecords.get(numRecordsDeserialized);
			
				assertTrue(deserializer.getNextRecord(next).isFullRecord());
				assertEquals(originalRecords.get(numRecordsDeserialized), next);
				numRecordsDeserialized++;
			}
			
			// might be that the last big records has not yet been fully moved, and a small one is missing
			assertFalse(serializer.hasData());
			assertFalse(deserializer.hasUnfinishedData());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
