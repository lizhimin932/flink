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

package org.apache.flink.streaming.runtime.operators.windows;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windows.KeyedWindowFunction;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.operators.TriggerTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import org.apache.flink.util.Collector;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class AccumulatingAlignedProcessingTimeWindowOperatorTest {

	@SuppressWarnings("unchecked")
	private final KeyedWindowFunction<String, String, String> mockFunction = mock(KeyedWindowFunction.class);

	@SuppressWarnings("unchecked")
	private final KeySelector<String, String> mockKeySelector = mock(KeySelector.class);
	
	private final KeySelector<Integer, Integer> identitySelector = new KeySelector<Integer, Integer>() {
		@Override
		public Integer getKey(Integer value) {
			return value;
		}
	};
	
	private final KeyedWindowFunction<Integer, Integer, Integer> validatingIdentityFunction = 
			new KeyedWindowFunction<Integer, Integer, Integer>()
	{
		@Override
		public void evaluate(Integer key, Iterable<Integer> values, Collector<Integer> out) {
			for (Integer val : values) {
				assertEquals(key, val);
				out.collect(val);
			}
		}
	};

	// ------------------------------------------------------------------------

	@After
	public void checkNoTriggerThreadsRunning() {
		// make sure that all the threads we trigger are shut down
		long deadline = System.currentTimeMillis() + 5000;
		while (TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() > 0 && System.currentTimeMillis() < deadline) {
			try {
				Thread.sleep(10);
			}
			catch (InterruptedException ignored) {}
		}

		assertTrue("Not all trigger threads where properly shut down",
				TriggerTimer.TRIGGER_THREADS_GROUP.activeCount() == 0);
	}
	
	// ------------------------------------------------------------------------
	
	@Test
	public void testInvalidParameters() {
		try {
			assertInvalidParameter(-1L, -1L);
			assertInvalidParameter(10000L, -1L);
			assertInvalidParameter(-1L, 1000L);
			assertInvalidParameter(1000L, 2000L);
			
			// actual internal slide is too low here:
			assertInvalidParameter(1000L, 999L);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testWindowSizeAndSlide() {
		try {
			AbstractAlignedProcessingTimeWindowOperator<String, String, String> op;
			
			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 5000, 1000);
			assertEquals(5000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(5, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1000, 1000);
			assertEquals(1000, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(1000, op.getPaneSize());
			assertEquals(1, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1500, 1000);
			assertEquals(1500, op.getWindowSize());
			assertEquals(1000, op.getWindowSlide());
			assertEquals(500, op.getPaneSize());
			assertEquals(3, op.getNumPanesPerWindow());

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1200, 1100);
			assertEquals(1200, op.getWindowSize());
			assertEquals(1100, op.getWindowSlide());
			assertEquals(100, op.getPaneSize());
			assertEquals(12, op.getNumPanesPerWindow());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testWindowTriggerTimeAlignment() {
		try {
			@SuppressWarnings("unchecked")
			final Output<StreamRecord<String>> mockOut = mock(Output.class);
			
			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			AbstractAlignedProcessingTimeWindowOperator<String, String, String> op;

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 5000, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1000, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 1000 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1500, 1000);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 500 == 0);
			assertTrue(op.getNextEvaluationTime() % 1000 == 0);
			op.dispose();

			op = new AccumulatingProcessingTimeWindowOperator<>(mockFunction, mockKeySelector, 1200, 1100);
			op.setup(mockOut, mockContext);
			op.open(new Configuration());
			assertTrue(op.getNextSlideTime() % 100 == 0);
			assertTrue(op.getNextEvaluationTime() % 1100 == 0);
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testTumblingWindow() {
		try {
			final int windowSize = 50;
			final CollectingOutput<Integer> out = new CollectingOutput<>(windowSize);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			// tumbling window that triggers every 20 milliseconds
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							validatingIdentityFunction, identitySelector, windowSize, windowSize);

			op.setup(out, mockContext);
			op.open(new Configuration());

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				op.processElement(new StreamRecord<Integer>(i));
				Thread.sleep(1);
			}

			op.close();
			op.dispose();

			// get and verify the result
			List<Integer> result = out.getElements();
			assertEquals(numElements, result.size());

			Collections.sort(result);
			for (int i = 0; i < numElements; i++) {
				assertEquals(i, result.get(i).intValue());
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingWindow() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>(50);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			// tumbling window that triggers every 20 milliseconds
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(validatingIdentityFunction, identitySelector, 150, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());

			final int numElements = 1000;

			for (int i = 0; i < numElements; i++) {
				op.processElement(new StreamRecord<Integer>(i));
				Thread.sleep(1);
			}

			op.close();
			op.dispose();

			// get and verify the result
			List<Integer> result = out.getElements();

			// if we kept this running, each element would be in the result three times (for each slide).
			// we are closing the window before the final panes are through three times, so we may have less
			// elements.
			if (result.size() < numElements || result.size() > 3 * numElements) {
				fail("Wrong number of results: " + result.size());
			}

			Collections.sort(result);
			int lastNum = -1;
			int lastCount = -1;
			
			for (int num : result) {
				if (num == lastNum) {
					lastCount++;
					assertTrue(lastCount <= 3);
				}
				else {
					lastNum = num;
					lastCount = 1;
				}
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testSlidingWindowSingleElements() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>(50);

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			// tumbling window that triggers every 20 milliseconds
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(validatingIdentityFunction, identitySelector, 150, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());
			
			op.processElement(new StreamRecord<Integer>(1));
			op.processElement(new StreamRecord<Integer>(2));

			// each element should end up in the output three times
			// wait until the elements have arrived 6 times in the output
			long deadline = System.currentTimeMillis() + 6000000;
			do {
				Thread.sleep(50);
			}
			while(out.getElements().size() < 6 && System.currentTimeMillis() < deadline);
			
			List<Integer> result = out.getElements();
			assertEquals(6, result.size());
			
			Collections.sort(result);
			assertEquals(Arrays.asList(1, 1, 1, 2, 2, 2), result);
			
			op.close();
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	@Test
	public void testEmitTrailingDataOnClose() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");
			
			// the operator has a window time that is so long that it will not fire in this test
			final long oneYear = 365L * 24 * 60 * 60 * 1000;
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op = 
					new AccumulatingProcessingTimeWindowOperator<>(validatingIdentityFunction, identitySelector,
							oneYear, oneYear);
			
			op.setup(out, mockContext);
			op.open(new Configuration());
			
			List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
			for (Integer i : data) {
				op.processElement(new StreamRecord<Integer>(i));
			}
			
			op.close();
			op.dispose();
			
			// get and verify the result
			List<Integer> result = out.getElements();
			Collections.sort(result);
			assertEquals(data, result);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropagateExceptionsFromTrigger() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			KeyedWindowFunction<Integer, Integer, Integer> failingFunction = new FailingFunction(100);
			
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(failingFunction, identitySelector, 50, 50);

			op.setup(out, mockContext);
			op.open(new Configuration());

			try {
				int num = 0;
				while (num < Integer.MAX_VALUE) {
					op.processElement(new StreamRecord<Integer>(num++));
					Thread.sleep(1);
				}
				fail("This should really have failed with an exception quite a while ago...");
			}
			catch (Exception e) {
				assertNotNull(e.getCause());
				assertTrue(e.getCause().getMessage().contains("Artificial Test Exception"));
			}
			
			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testPropagateExceptionsFromClose() {
		try {
			final CollectingOutput<Integer> out = new CollectingOutput<>();

			final StreamingRuntimeContext mockContext = mock(StreamingRuntimeContext.class);
			when(mockContext.getTaskName()).thenReturn("Test task name");

			KeyedWindowFunction<Integer, Integer, Integer> failingFunction = new FailingFunction(100);

			// the operator has a window time that is so long that it will not fire in this test
			final long hundredYears = 100L * 365 * 24 * 60 * 60 * 1000;
			AbstractAlignedProcessingTimeWindowOperator<Integer, Integer, Integer> op =
					new AccumulatingProcessingTimeWindowOperator<>(
							failingFunction, identitySelector, hundredYears, hundredYears);

			op.setup(out, mockContext);
			op.open(new Configuration());

			for (int i = 0; i < 150; i++) {
				op.processElement(new StreamRecord<Integer>(i));
			}
			
			try {
				op.close();
				fail("This should fail with an exception");
			}
			catch (Exception e) {
				assertTrue(
						e.getMessage().contains("Artificial Test Exception") ||
						(e.getCause() != null && e.getCause().getMessage().contains("Artificial Test Exception")));
			}

			op.dispose();
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// ------------------------------------------------------------------------
	
	private void assertInvalidParameter(long windowSize, long windowSlide) {
		try {
			new AccumulatingProcessingTimeWindowOperator<String, String, String>(
					mockFunction, mockKeySelector, windowSize, windowSlide);
			fail("This should fail with an IllegalArgumentException");
		}
		catch (IllegalArgumentException e) {
			// expected
		}
		catch (Exception e) {
			fail("Wrong exception. Expected IllegalArgumentException but found " + e.getClass().getSimpleName());
		}
	}

	// ------------------------------------------------------------------------
	
	private static class FailingFunction implements KeyedWindowFunction<Integer, Integer, Integer> {

		private final int failAfterElements;
		
		private int numElements;

		FailingFunction(int failAfterElements) {
			this.failAfterElements = failAfterElements;
		}

		@Override
		public void evaluate(Integer integer, Iterable<Integer> values, Collector<Integer> out) throws Exception {
			for (Integer i : values) {
				out.collect(i);
				numElements++;
				
				if (numElements >= failAfterElements) {
					throw new Exception("Artificial Test Exception");
				}
			}
		}
	}
}
