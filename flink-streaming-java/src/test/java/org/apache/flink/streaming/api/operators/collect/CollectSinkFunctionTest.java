/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.SerializedListAccumulator;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.api.operators.collect.utils.CheckpointedCollectClient;
import org.apache.flink.streaming.api.operators.collect.utils.CollectRequestSender;
import org.apache.flink.streaming.api.operators.collect.utils.MockFunctionInitializationContext;
import org.apache.flink.streaming.api.operators.collect.utils.MockOperatorEventGateway;
import org.apache.flink.streaming.api.operators.collect.utils.TestCollectClient;
import org.apache.flink.streaming.api.operators.collect.utils.UncheckpointedCollectClient;
import org.apache.flink.streaming.util.MockStreamingRuntimeContext;
import org.apache.flink.types.Row;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

/**
 * Tests for {@link CollectSinkFunction}.
 */
public class CollectSinkFunctionTest extends TestLogger {

	private static final int MAX_RESULTS_PER_BATCH = 3;
	private static final String LIST_ACC_NAME = "tableCollectList";
	private static final String OFFSET_ACC_NAME = "tableCollectOffset";

	private static final TypeSerializer<Row> serializer =
		new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO).createSerializer(new ExecutionConfig());

	private CollectSinkFunction<Row> function;
	private CollectSinkOperatorCoordinator coordinator;
	private boolean jobFinished;

	private StreamingRuntimeContext runtimeContext;
	private MockOperatorEventGateway gateway;

	@Before
	public void before() throws Exception {
		runtimeContext = new MockStreamingRuntimeContext(false, 1, 0);
		gateway = new MockOperatorEventGateway();
		function = new CollectSinkFunction<>(serializer, MAX_RESULTS_PER_BATCH, LIST_ACC_NAME, OFFSET_ACC_NAME);
		coordinator = new CollectSinkOperatorCoordinator();
		coordinator.start();

		function.setRuntimeContext(runtimeContext);
		function.setOperatorEventGateway(gateway);

		jobFinished = false;
	}

	@After
	public void after() throws Exception {
		coordinator.close();
	}

	@Test
	public void testUncheckpointedProtocol() throws Exception {
		openFunction();
		for (int i = 0; i < 6; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequest("", 0);
		Assert.assertEquals(0, response.getOffset());
		String version = response.getVersion();

		response = sendRequest(version, 0);
		assertResponseEquals(response, version, 0, Long.MIN_VALUE, Arrays.asList(0, 1, 2));

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, Long.MIN_VALUE, Arrays.asList(4, 5));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 6; i < 10; i++) {
			function.invoke(Row.of(i), null);
		}

		// invalid request
		response = sendRequest(version, 5);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Collections.emptyList());

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(6, 7, 8));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(6, 7, 8));

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 10, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 10; i < 16; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Arrays.asList(12, 13, 14));

		finishJob();

		assertAccumulatorResult(12, Arrays.asList(12, 13, 14, 15));
	}

	@Test
	public void testCheckpointProtocol() throws Exception {
		FunctionInitializationContext functionInitializationContext = new MockFunctionInitializationContext();
		function.initializeState(functionInitializationContext);
		openFunction();
		for (int i = 0; i < 2; i++) {
			// CollectSinkFunction never use context when invoked
			function.invoke(Row.of(i), null);
		}

		CollectCoordinationResponse<Row> response = sendRequest("", 0);
		Assert.assertEquals(0, response.getOffset());
		String version = response.getVersion();

		response = sendRequest(version, 0);
		assertResponseEquals(response, version, 0, Long.MIN_VALUE, Arrays.asList(0, 1));

		for (int i = 2; i < 6; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 3);
		assertResponseEquals(response, version, 3, Long.MIN_VALUE, Arrays.asList(3, 4, 5));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(0);

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, 0, Arrays.asList(4, 5));

		for (int i = 6; i < 9; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, 0, Arrays.asList(6, 7, 8));

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		for (int i = 9; i < 12; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 4);
		Assert.assertEquals(3, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 4);
		assertResponseEquals(response, version, 4, Long.MIN_VALUE, Arrays.asList(4, 5, 9));

		response = sendRequest(version, 6);
		assertResponseEquals(response, version, 6, Long.MIN_VALUE, Arrays.asList(9, 10, 11));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(1);

		function.invoke(Row.of(12), null);

		response = sendRequest(version, 7);
		assertResponseEquals(response, version, 7, 1, Arrays.asList(10, 11, 12));

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		response = sendRequest(version, 7);
		Assert.assertEquals(6, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 7);
		assertResponseEquals(response, version, 7, Long.MIN_VALUE, Arrays.asList(10, 11));

		response = sendRequest(version, 9);
		assertResponseEquals(response, version, 9, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 13; i < 16; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 9);
		assertResponseEquals(response, version, 9, Long.MIN_VALUE, Arrays.asList(13, 14, 15));

		// CollectSinkFunction is not using this context
		function.snapshotState(null);
		function.notifyCheckpointComplete(2);

		// this is an exceptional shutdown
		function.close();

		function.initializeState(functionInitializationContext);
		openFunction();

		response = sendRequest(version, 12);
		Assert.assertEquals(9, response.getOffset());
		version = response.getVersion();

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Collections.emptyList());

		for (int i = 16; i < 20; i++) {
			function.invoke(Row.of(i), null);
		}

		response = sendRequest(version, 12);
		assertResponseEquals(response, version, 12, Long.MIN_VALUE, Arrays.asList(16, 17, 18));

		finishJob();

		assertAccumulatorResult(12, Arrays.asList(16, 17, 18, 19));
	}

	@Test
	public void testUncheckpointedFunctionWithUncheckpointedClient() throws Exception {
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			expected.add(i);
		}
		DataFeeder feeder = new DataFeeder(expected, false);
		UncheckpointedCollectClient<Row> client = new UncheckpointedCollectClient<>(
			serializer,
			new TestCollectRequestSender(),
			() -> jobFinished);

		runFunctionWithClient(feeder, client);
		assertResultsEqualAfterSort(expected, client.getResults());
	}

	@Test
	public void testUncheckpointedFunctionWithCheckpointedClient() throws Exception {
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			expected.add(i);
		}
		DataFeeder feeder = new DataFeeder(expected, false);
		CheckpointedCollectClient<Row> client = new CheckpointedCollectClient<>(
			serializer,
			new TestCollectRequestSender(),
			() -> jobFinished);

		runFunctionWithClient(feeder, client);
		assertResultsEqualAfterSort(expected, client.getResults());
	}

	@Test
	public void testCheckpointedFunctionWithCheckpointedClient() throws Exception {
		List<Integer> expected = new ArrayList<>();
		for (int i = 0; i < 100; i++) {
			expected.add(i);
		}
		DataFeeder feeder = new DataFeeder(expected, true);
		CheckpointedCollectClient<Row> client = new CheckpointedCollectClient<>(
			serializer,
			new TestCollectRequestSender(),
			() -> jobFinished);

		runFunctionWithClient(feeder, client);
		assertResultsEqualAfterSort(expected, client.getResults());
	}

	private void runFunctionWithClient(DataFeeder feeder, TestCollectClient<Row> client) throws Exception {
		Thread feederThread = new Thread(feeder);
		Thread clientThread = new Thread(client);

		Thread.UncaughtExceptionHandler exceptionHandler = (t, e) -> {
			feederThread.interrupt();
			clientThread.interrupt();
		};
		feederThread.setUncaughtExceptionHandler(exceptionHandler);
		clientThread.setUncaughtExceptionHandler(exceptionHandler);

		feederThread.start();
		clientThread.start();
		feederThread.join();
		clientThread.join();
	}

	private void openFunction() throws Exception {
		function.open(new Configuration());
		coordinator.handleEventFromOperator(0, gateway.getNextEvent());
	}

	private void finishJob() throws Exception {
		// this is a normal shutdown
		function.accumulateFinalResults();
		function.close();

		jobFinished = true;
	}

	@SuppressWarnings("unchecked")
	private CollectCoordinationResponse<Row> sendRequest(
			String version,
			long offset) throws Exception {
		CollectCoordinationRequest request = new CollectCoordinationRequest(version, offset);
		return ((CollectCoordinationResponse) coordinator.handleCoordinationRequest(request).get());
	}

	@SuppressWarnings("unchecked")
	private Tuple2<List<Row>, Long> getAccumualtorResults() throws Exception {
		Accumulator listAccumulator = runtimeContext.getAccumulator(LIST_ACC_NAME);
		ArrayList<byte[]> serializedResult = ((SerializedListAccumulator) listAccumulator).getLocalValue();
		List<Row> results = SerializedListAccumulator.deserializeList(serializedResult, serializer);

		Accumulator offsetAccumulator = runtimeContext.getAccumulator(OFFSET_ACC_NAME);
		long offset = ((LongCounter) offsetAccumulator).getLocalValue();

		return Tuple2.of(results, offset);
	}

	private void assertResponseEquals(
			CollectCoordinationResponse<Row> response,
			String version,
			long offset,
			long lastCheckpointId,
			List<Integer> expected) throws IOException {
		Assert.assertEquals(version, response.getVersion());
		Assert.assertEquals(offset, response.getOffset());
		Assert.assertEquals(lastCheckpointId, response.getLastCheckpointId());
		List<Row> results = response.getResults(serializer);
		assertResultsEqual(expected, results);
	}

	private void assertResultsEqual(List<Integer> expected, List<Row> actual) {
		Assert.assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			Row row = actual.get(i);
			Assert.assertEquals(1, row.getArity());
			Assert.assertEquals(expected.get(i), row.getField(0));
		}
	}

	private void assertResultsEqualAfterSort(List<Integer> expected, List<Row> actual) {
		Collections.sort(expected);
		actual.sort(Comparator.comparingInt(row -> (int) row.getField(0)));
		assertResultsEqual(expected, actual);
	}

	private void assertAccumulatorResult(long expectedOffset, List<Integer> expectedResults) throws Exception {
		Tuple2<List<Row>, Long> accResults = getAccumualtorResults();
		Assert.assertEquals(expectedResults.size(), accResults.f0.size());
		Assert.assertEquals(expectedOffset, accResults.f1.longValue());
	}

	private class TestCollectRequestSender implements CollectRequestSender<Row> {

		@Override
		public CollectCoordinationResponse<Row> sendRequest(String version, long offset) throws Exception {
			return CollectSinkFunctionTest.this.sendRequest(version, offset);
		}

		@Override
		public Tuple2<List<Row>, Long> getAccumulatorResults() throws Exception {
			return CollectSinkFunctionTest.this.getAccumualtorResults();
		}
	}

	private class DataFeeder implements Runnable {

		private final boolean checkpointed;

		private LinkedList<Integer> data;
		private List<Integer> checkpointedData;
		private long checkpointId;
		private FunctionInitializationContext functionInitializationContext;

		private DataFeeder(List<Integer> data, boolean checkpointed) {
			this.checkpointed = checkpointed;

			this.data = new LinkedList<>(data);
			this.checkpointedData = new ArrayList<>(data);
			this.checkpointId = 0;
			this.functionInitializationContext = new MockFunctionInitializationContext();
		}

		@Override
		public void run() {
			Random random = new Random();

			try {
				function.initializeState(functionInitializationContext);
				openFunction();

				while (data.size() > 0) {
					int r = random.nextInt() % (checkpointed ? 5 : 3);
					if (r < 3) {
						// with 60% chance we add some data
						int size = Math.min(data.size(), random.nextInt(MAX_RESULTS_PER_BATCH * 3) + 1);
						for (int i = 0; i < size; i++) {
							function.invoke(Row.of(data.removeFirst()), null);
						}
					} else if (r < 4) {
						// with 20% chance we make a checkpoint
						checkpointId++;
						checkpointedData = new ArrayList<>(data);

						function.snapshotState(null);
						function.notifyCheckpointComplete(checkpointId);
					} else {
						// with 20% chance we fail
						// we shuffle data to emulate jobs whose result order is undetermined
						Collections.shuffle(checkpointedData);
						data = new LinkedList<>(checkpointedData);

						function.close();
						function.initializeState(functionInitializationContext);
						openFunction();
					}

					if (random.nextBoolean()) {
						Thread.sleep(random.nextInt(100));
					}
				}

				finishJob();
			} catch (Exception e) {
				Assert.fail("Exception occurs in DataFeeder");
				e.printStackTrace();
				throw new RuntimeException(e);
			}
		}
	}
}
