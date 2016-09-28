/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.util;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.RunnableFuture;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * A test harness for testing a {@link OneInputStreamOperator}.
 *
 * <p>
 * This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. You are free to modify these.
 */
public class OneInputStreamOperatorTestHarness<IN, OUT> {

	protected static final int MAX_PARALLELISM = 10;

	final OneInputStreamOperator<IN, OUT> operator;

	final ConcurrentLinkedQueue<Object> outputList;

	final StreamConfig config;

	final ExecutionConfig executionConfig;

	final TestProcessingTimeService processingTimeService;

	StreamTask<?, ?> mockTask;

	// use this as default for tests
	AbstractStateBackend stateBackend = new MemoryStateBackend();

	private final Object checkpointLock;

	/**
	 * Whether setup() was called on the operator. This is reset when calling close().
	 */
	private boolean setupCalled = false;

	private volatile boolean wasFailedExternally = false;

	public OneInputStreamOperatorTestHarness(OneInputStreamOperator<IN, OUT> operator) throws Exception {
		this(operator, new ExecutionConfig());
	}

	public OneInputStreamOperatorTestHarness(
			OneInputStreamOperator<IN, OUT> operator,
			ExecutionConfig executionConfig) throws Exception {
		this.operator = operator;
		this.outputList = new ConcurrentLinkedQueue<>();
		Configuration underlyingConfig = new Configuration();
		this.config = new StreamConfig(underlyingConfig);
		this.config.setCheckpointingEnabled(true);
		this.executionConfig = executionConfig;

		this.checkpointLock = new Object();

		final Environment env = new MockEnvironment("MockTwoInputTask", 3 * 1024 * 1024, new MockInputSplitProvider(), 1024, underlyingConfig, executionConfig, MAX_PARALLELISM, 1, 0);
		mockTask = mock(StreamTask.class);
		processingTimeService = new TestProcessingTimeService();
		processingTimeService.setCurrentTime(0);

		when(mockTask.getName()).thenReturn("Mock Task");
		when(mockTask.getCheckpointLock()).thenReturn(checkpointLock);
		when(mockTask.getConfiguration()).thenReturn(config);
		when(mockTask.getTaskConfiguration()).thenReturn(underlyingConfig);
		when(mockTask.getEnvironment()).thenReturn(env);
		when(mockTask.getExecutionConfig()).thenReturn(executionConfig);
		when(mockTask.getUserCodeClassLoader()).thenReturn(this.getClass().getClassLoader());

		doAnswer(new Answer<Void>() {
			@Override
			public Void answer(InvocationOnMock invocation) throws Throwable {
				wasFailedExternally = true;
				return null;
			}
		}).when(mockTask).handleAsyncException(any(String.class), any(Throwable.class));

		try {
			doAnswer(new Answer<CheckpointStreamFactory>() {
				@Override
				public CheckpointStreamFactory answer(InvocationOnMock invocationOnMock) throws Throwable {

					final StreamOperator<?> operator = (StreamOperator<?>) invocationOnMock.getArguments()[0];
					return stateBackend.createStreamFactory(new JobID(), operator.getClass().getSimpleName());
				}
			}).when(mockTask).createCheckpointStreamFactory(any(StreamOperator.class));
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}

		doAnswer(new Answer<ProcessingTimeService>() {
			@Override
			public ProcessingTimeService answer(InvocationOnMock invocation) throws Throwable {
				return processingTimeService;
			}
		}).when(mockTask).getProcessingTimeService();
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.config.setTimeCharacteristic(timeCharacteristic);
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return this.config.getTimeCharacteristic();
	}

	public boolean wasFailedExternally() {
		return wasFailedExternally;
	}

	public void setStateBackend(AbstractStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public Object getCheckpointLock() {
		return mockTask.getCheckpointLock();
	}

	public Environment getEnvironment() {
		return this.mockTask.getEnvironment();
	}

	/**
	 * Get all the output from the task. This contains StreamRecords and Events interleaved.
	 */
	public ConcurrentLinkedQueue<Object> getOutput() {
		return outputList;
	}

	/**
	 * Get all the output from the task and clear the output buffer.
	 * This contains only StreamRecords.
	 */
	@SuppressWarnings("unchecked")
	public List<StreamRecord<? extends OUT>> extractOutputStreamRecords() {
		List<StreamRecord<? extends OUT>> resultElements = new LinkedList<>();
		for (Object e: getOutput()) {
			if (e instanceof StreamRecord) {
				resultElements.add((StreamRecord<OUT>) e);
			}
		}
		return resultElements;
	}

	/**
	 * Calls
	 * {@link org.apache.flink.streaming.api.operators.StreamOperator#setup(StreamTask, StreamConfig, Output)} ()}
	 */
	public void setup() throws Exception {
		operator.setup(mockTask, config, new MockOutput());
		setupCalled = true;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open()}. This also
	 * calls {@link org.apache.flink.streaming.api.operators.StreamOperator#setup(StreamTask, StreamConfig, Output)}
	 * if it was not called before.
	 */
	public void open() throws Exception {
		if (!setupCalled) {
			setup();
		}
		operator.open();
	}

	/**
	 *
	 */
	public StreamStateHandle snapshot(long checkpointId, long timestamp) throws Exception {
		synchronized (checkpointLock) {
			CheckpointStreamFactory.CheckpointStateOutputStream outStream = stateBackend.createStreamFactory(
					new JobID(),
					"test_op").createCheckpointStateOutputStream(checkpointId, timestamp);

			if (operator instanceof StreamCheckpointedOperator) {
				((StreamCheckpointedOperator) operator).snapshotState(
						outStream,
						checkpointId,
						timestamp);
			}

			RunnableFuture<OperatorStateHandle> snapshotRunnable = operator.snapshotState(
					checkpointId,
					timestamp,
					stateBackend.createStreamFactory(new JobID(), "test_op"));

			if (snapshotRunnable != null) {
				outStream.write(1);
				snapshotRunnable.run();
				OperatorStateHandle operatorStateHandle = snapshotRunnable.get();

				InstantiationUtil.serializeObject(outStream, operatorStateHandle);
			} else {
				outStream.write(0);
			}

			return outStream.closeAndGetHandle();
		}
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#notifyOfCompletedCheckpoint(long)} ()}
	 */
	public void notifyOfCompletedCheckpoint(long checkpointId) throws Exception {
		operator.notifyOfCompletedCheckpoint(checkpointId);
	}

	/**
	 *
	 */
	public void restore(StreamStateHandle snapshot) throws Exception {
		try (FSDataInputStream in = snapshot.openInputStream()) {

			if (operator instanceof StreamCheckpointedOperator) {
				((StreamCheckpointedOperator) operator).restoreState(in);
			}

			int hasOperatorState = in.read();

			if (hasOperatorState == 1) {
				OperatorStateHandle operatorStateHandle =
						InstantiationUtil.deserializeObject(
								in,
								this.getClass().getClassLoader());

				operator.restoreState(Collections.singletonList(operatorStateHandle));
			}
		}
	}

	/**
	 * Calls close and dispose on the operator.
	 */
	public void close() throws Exception {
		operator.close();
		operator.dispose();
		if (processingTimeService != null) {
			processingTimeService.shutdownService();
		}
		setupCalled = false;
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.setKeyContextElement1(element);
		operator.processElement(element);
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			operator.setKeyContextElement1(element);
			operator.processElement(element);
		}
	}

	public void setProcessingTime(long time) throws Exception {
		synchronized (checkpointLock) {
			processingTimeService.setCurrentTime(time);
		}
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		private TypeSerializer<OUT> outputSerializer;

		@Override
		public void emitWatermark(Watermark mark) {
			outputList.add(mark);
		}

		@Override
		public void collect(StreamRecord<OUT> element) {
			if (outputSerializer == null) {
				outputSerializer = TypeExtractor.getForObject(element.getValue()).createSerializer(executionConfig);
			}
			outputList.add(new StreamRecord<OUT>(outputSerializer.copy(element.getValue()),
					element.getTimestamp()));
		}

		@Override
		public void close() {
			// ignore
		}
	}
}
