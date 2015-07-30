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

package org.apache.flink.test.checkpointing;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.test.util.ForkableFlinkMiniCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;


/**
 * A simple test that runs a streaming topology with checkpointing enabled.
 * 
 * The test triggers a failure after a while and verifies that, after completion, the
 * state reflects the "exactly once" semantics.
 */
@SuppressWarnings("serial")
public class StreamCheckpointingITCase {

	private static final int NUM_TASK_MANAGERS = 2;
	private static final int NUM_TASK_SLOTS = 3;
	private static final int PARALLELISM = NUM_TASK_MANAGERS * NUM_TASK_SLOTS;

	private static ForkableFlinkMiniCluster cluster;

	@BeforeClass
	public static void startCluster() {
		try {
			Configuration config = new Configuration();
			config.setInteger(ConfigConstants.LOCAL_INSTANCE_MANAGER_NUMBER_TASK_MANAGER, NUM_TASK_MANAGERS);
			config.setInteger(ConfigConstants.TASK_MANAGER_NUM_TASK_SLOTS, NUM_TASK_SLOTS);
			config.setString(ConfigConstants.DEFAULT_EXECUTION_RETRY_DELAY_KEY, "0 ms");
			config.setInteger(ConfigConstants.TASK_MANAGER_MEMORY_SIZE_KEY, 12);
			
			cluster = new ForkableFlinkMiniCluster(config, false);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to start test cluster: " + e.getMessage());
		}
	}

	@AfterClass
	public static void shutdownCluster() {
		try {
			cluster.shutdown();
			cluster = null;
		}
		catch (Exception e) {
			e.printStackTrace();
			fail("Failed to stop test cluster: " + e.getMessage());
		}
	}


	/**
	 * Runs the following program:
	 *
	 * <pre>
	 *     [ (source)->(filter) ]-s->[ (map) ] -> [ (map) ] -> [ (groupBy/count)->(sink) ]
	 * </pre>
	 */
	@Test
	public void runCheckpointedProgram() {

		final long NUM_STRINGS = 10000000L;
		assertTrue("Broken test setup", NUM_STRINGS % 40 == 0);
		
		try {
			StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
																	"localhost", cluster.getJobManagerRPCPort());
			env.setParallelism(PARALLELISM);
			env.enableCheckpointing(500);
			env.getConfig().disableSysoutLogging();

			DataStream<String> stream = env.addSource(new StringGeneratingSourceFunction(NUM_STRINGS));
			
			stream
					// -------------- first vertex, chained to the source ----------------
					.filter(new StringRichFilterFunction()).shuffle()

					// -------------- seconds vertex - the stateful one that also fails ----------------
					.map(new StringPrefixCountRichMapFunction())
					.startNewChain()
					.map(new StatefulCounterFunction())

					// -------------- third vertex - counter and the sink ----------------
					.groupBy("prefix")
					.map(new OnceFailingPrefixCounter(NUM_STRINGS))
					.addSink(new SinkFunction<PrefixCount>() {

						@Override
						public void invoke(PrefixCount value) throws Exception {
							// Do nothing here
						}
					});

			env.execute();
			
			long filterSum = 0;
			for (long l : StringRichFilterFunction.counts) {
				filterSum += l;
			}

			long mapSum = 0;
			for (long l : StringPrefixCountRichMapFunction.counts) {
				mapSum += l;
			}

			long countSum = 0;
			for (long l : StatefulCounterFunction.counts) {
				countSum += l;
			}
			
			long reduceInputCount = 0;
			for(long l: OnceFailingPrefixCounter.counts){
				reduceInputCount += l;
			}
			
			assertEquals(NUM_STRINGS, filterSum);
			assertEquals(NUM_STRINGS, mapSum);
			assertEquals(NUM_STRINGS, countSum);
			assertEquals(NUM_STRINGS, reduceInputCount);
			// verify that we counted exactly right
			for (Long count : OnceFailingPrefixCounter.prefixCounts.values()) {
				assertEquals(new Long(NUM_STRINGS / 40), count);
			}
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Functions
	// --------------------------------------------------------------------------------------------
	
	private static class StringGeneratingSourceFunction extends RichSourceFunction<String>
			implements  ParallelSourceFunction<String> {

		private final long numElements;
		
		private Random rnd;
		private StringBuilder stringBuilder;

		private OperatorState<Integer> index;
		private int step;

		private volatile boolean isRunning;

		static final long[] counts = new long[PARALLELISM];
		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = index.value();
		}


		StringGeneratingSourceFunction(long numElements) {
			this.numElements = numElements;
		}

		@Override
		public void open(Configuration parameters) throws IOException {
			rnd = new Random();
			stringBuilder = new StringBuilder();
			step = getRuntimeContext().getNumberOfParallelSubtasks();
			
			
			index = getRuntimeContext().getOperatorState("index", getRuntimeContext().getIndexOfThisSubtask(), false);
			
			isRunning = true;
		}

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
			final Object lockingObject = ctx.getCheckpointLock();

			while (isRunning && index.value() < numElements) {
				char first = (char) ((index.value() % 40) + 40);

				stringBuilder.setLength(0);
				stringBuilder.append(first);

				String result = randomString(stringBuilder, rnd);

				synchronized (lockingObject) {
					index.update(index.value() + step);
					ctx.collect(result);
				}
			}
		}

		@Override
		public void cancel() {
			isRunning = false;
		}

		private static String randomString(StringBuilder bld, Random rnd) {
			final int len = rnd.nextInt(10) + 5;

			for (int i = 0; i < len; i++) {
				char next = (char) (rnd.nextInt(20000) + 33);
				bld.append(next);
			}

			return bld.toString();
		}
	}
	
	private static class StatefulCounterFunction extends RichMapFunction<PrefixCount, PrefixCount> {

		private OperatorState<Long> count;
		static final long[] counts = new long[PARALLELISM];

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			count.update(count.value() + 1);
			return value;
		}

		@Override
		public void open(Configuration conf) throws IOException {
			count = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count.value();
		}
		
	}
	
	private static class OnceFailingPrefixCounter extends RichMapFunction<PrefixCount, PrefixCount> {
		
		private static Map<String, Long> prefixCounts = new ConcurrentHashMap<String, Long>();
		static final long[] counts = new long[PARALLELISM];

		private static volatile boolean hasFailed = false;

		private final long numElements;
		
		private long failurePos;
		private long count;
		
		private OperatorState<Long> pCount;
		private OperatorState<Long> inputCount;

		OnceFailingPrefixCounter(long numElements) {
			this.numElements = numElements;
		}
		
		@Override
		public void open(Configuration parameters) throws IOException {
			long failurePosMin = (long) (0.4 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());
			long failurePosMax = (long) (0.7 * numElements / getRuntimeContext().getNumberOfParallelSubtasks());

			failurePos = (new Random().nextLong() % (failurePosMax - failurePosMin)) + failurePosMin;
			count = 0;
			pCount = getRuntimeContext().getOperatorState("prefix-count", 0L, true);
			inputCount = getRuntimeContext().getOperatorState("input-count", 0L, false);
		}
		
		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = inputCount.value();
		}

		@Override
		public PrefixCount map(PrefixCount value) throws Exception {
			count++;
			if (!hasFailed && count >= failurePos) {
				hasFailed = true;
				throw new Exception("Test Failure");
			}
			inputCount.update(inputCount.value() + 1);
		
			long currentPrefixCount = pCount.value() + value.count;
			pCount.update(currentPrefixCount);
			prefixCounts.put(value.prefix, currentPrefixCount);
			value.count = currentPrefixCount;
			return value;
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//  Custom Type Classes
	// --------------------------------------------------------------------------------------------

	public static class PrefixCount implements Serializable {

		public String prefix;
		public String value;
		public long count;

		public PrefixCount() {}

		public PrefixCount(String prefix, String value, long count) {
			this.prefix = prefix;
			this.value = value;
			this.count = count;
		}

		@Override
		public String toString() {
			return prefix + " / " + value;
		}
	}

	private static class StringRichFilterFunction extends RichFilterFunction<String> implements Checkpointed<Long> {

		Long count = 0L;
		static final long[] counts = new long[PARALLELISM];
		
		@Override
		public boolean filter(String value) {
			count++;
			return value.length() < 100;
		}

		@Override
		public void close() {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count;
		}

		@Override
		public Long snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return count;
		}

		@Override
		public void restoreState(Long state) {
			count = state;
		}
	}

	private static class StringPrefixCountRichMapFunction extends RichMapFunction<String, PrefixCount>
			implements Checkpointed<Integer> {

		OperatorState<Long> count;
		static final long[] counts = new long[PARALLELISM];
		
		@Override
		public PrefixCount map(String value) throws IOException {
			count.update(count.value() + 1);
			return new PrefixCount(value.substring(0, 1), value, 1L);
		}
		
		@Override
		public void open(Configuration conf) throws IOException {
			this.count = getRuntimeContext().getOperatorState("count", 0L, false);
		}

		@Override
		public void close() throws IOException {
			counts[getRuntimeContext().getIndexOfThisSubtask()] = count.value();
		}

		@Override
		public Integer snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
			return null;
		}

		@Override
		public void restoreState(Integer state) {
			// verify that we never store/restore null state
			fail();
		}
	}
}
