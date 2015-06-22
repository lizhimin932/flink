/**
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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.state.LocalStateHandle;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamingRuntimeContext;

import java.io.Serializable;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;

/**
 * A mock {@link org.apache.flink.streaming.runtime.tasks.StreamTask} that can be used
 * to test a {@link org.apache.flink.streaming.api.operators.OneInputStreamOperator}.
 *
 * <p>
 * This mock task provides the operator with a basic runtime context and allows pushing elements
 * and watermarks into the operator. {@link java.util.Deque}s containing the emitted elements
 * and watermarks can be retrieved. you are free to modify these.
 */
public class MockOneInputTask<IN, OUT> {

	OneInputStreamOperator<IN, OUT> operator;

	Deque<StreamRecord<OUT>> emittedElements;
	Deque<OUT> rawEmittedElements;
	Deque<Watermark> emittedWatermarks;

	ExecutionConfig executionConfig;

	private TypeSerializer<OUT> outputSerializer;

	public MockOneInputTask(OneInputStreamOperator<IN, OUT> operator) {
		this.operator = operator;

		emittedElements = new LinkedList<StreamRecord<OUT>>();
		rawEmittedElements = new LinkedList<OUT>();
		emittedWatermarks = new LinkedList<Watermark>();

		executionConfig = new ExecutionConfig();

		StreamingRuntimeContext runtimeContext =  new StreamingRuntimeContext(
				"MockTwoInputTask",
				new MockEnvironment(3 * 1024 * 1024, new MockInputSplitProvider(), 1024),
				getClass().getClassLoader(),
				executionConfig,
				null,
				new LocalStateHandle.LocalStateHandleProvider<Serializable>());

		operator.setup(new MockOutput(), runtimeContext);
	}

	/**
	 * Returns the {@link java.util.Deque} to which emitted {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord}s are written.
	 * You can modify this. Modifications will not be reflected in the {@link java.util.Deque} returned
	 * from {@link #getRawEmittedElements()}.
	 */
	public Deque<StreamRecord<OUT>> getEmittedElements() {
		return emittedElements;
	}

	/**
	 * Returns the {@link java.util.Deque} to which emitted elements are written.
	 * You can modify this. Modifications will not be reflected in the {@link java.util.Deque} returned
	 * from {@link #getEmittedElements()}.
	 */
	public Deque<OUT> getRawEmittedElements() {
		return rawEmittedElements;
	}

	/**
	 * Returns the {@link java.util.Deque} to which emitted {@link org.apache.flink.streaming.api.watermark.Watermark}s are written.
	 * You can modify this.
	 */
	public Deque<Watermark> getEmittedWatermarks() {
		return emittedWatermarks;
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open(org.apache.flink.configuration.Configuration)}
	 * with an empty {@link org.apache.flink.configuration.Configuration}.
	 */
	public void open() throws Exception {
		operator.open(new Configuration());
	}

	/**
	 * Calls {@link org.apache.flink.streaming.api.operators.StreamOperator#open(org.apache.flink.configuration.Configuration)}
	 * with the given {@link org.apache.flink.configuration.Configuration}.
	 */
	public void open(Configuration config) throws Exception {
		operator.open(config);
	}

	/**
	 * Calls close on the operator.
	 */
	public void close() throws Exception {
		operator.close();
	}

	public void processElement(StreamRecord<IN> element) throws Exception {
		operator.processElement(element);
	}

	public void processElements(Collection<StreamRecord<IN>> elements) throws Exception {
		for (StreamRecord<IN> element: elements) {
			operator.processElement(element);
		}
	}

	public void processWatermark(Watermark mark) throws Exception {
		operator.processWatermark(mark);
	}

	private class MockOutput implements Output<StreamRecord<OUT>> {

		@Override
		public void emitWatermark(Watermark mark) {
			emittedWatermarks.add(mark);
		}

		@Override
		public void collect(StreamRecord<OUT> element) {
			if (outputSerializer == null) {
				outputSerializer = TypeExtractor.getForObject(element.getValue()).createSerializer(executionConfig);
			}
			emittedElements.add(new StreamRecord<OUT>(outputSerializer.copy(element.getValue()), element.getTimestamp()));
			rawEmittedElements.add(outputSerializer.copy(element.getValue()));
		}

		@Override
		public void close() {
			// ignore
		}
	}
}
