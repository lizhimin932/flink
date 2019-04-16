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

package org.apache.flink.table.runtime.sort;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.dataformat.BaseRow;
import org.apache.flink.table.dataformat.BinaryRow;
import org.apache.flink.table.dataformat.util.BaseRowUtil;
import org.apache.flink.table.generated.GeneratedNormalizedKeyComputer;
import org.apache.flink.table.generated.GeneratedRecordComparator;
import org.apache.flink.table.generated.NormalizedKeyComputer;
import org.apache.flink.table.generated.RecordComparator;
import org.apache.flink.table.runtime.TableStreamOperator;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.table.typeutils.BaseRowSerializer;
import org.apache.flink.table.typeutils.BaseRowTypeInfo;
import org.apache.flink.table.typeutils.BinaryRowSerializer;
import org.apache.flink.util.MutableObjectIterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Operator for stream sort.
 */
public class StreamSortOperator extends TableStreamOperator<BaseRow> implements
		OneInputStreamOperator<BaseRow, BaseRow> {

	private static final long serialVersionUID = 9042068324817807379L;

	private static final Logger LOG = LoggerFactory.getLogger(StreamSortOperator.class);

	private final BaseRowTypeInfo inputRowType;
	private final long memorySize;
	private GeneratedNormalizedKeyComputer gComputer;
	private GeneratedRecordComparator gComparator;

	private transient BinaryExternalSorter sorter;
	private transient StreamRecordCollector<BaseRow> collector;
	private transient BinaryRowSerializer binarySerializer;
	private transient BaseRowSerializer baseRowSerializer;

	// inputBuffer buffers all input elements, key is BaseRow, value is appear times.
	private transient HashMap<BaseRow, Long> inputBuffer;

	private transient ListState<Tuple2<BaseRow, Long>> bufferState;

	public StreamSortOperator(BaseRowTypeInfo inputRowType, long memorySize,
			GeneratedNormalizedKeyComputer gComputer, GeneratedRecordComparator gComparator) {
		this.inputRowType = inputRowType;
		this.memorySize = memorySize;
		this.gComputer = gComputer;
		this.gComparator = gComparator;
		this.inputBuffer = new HashMap<>();
	}

	@Override
	public void open() throws Exception {
		super.open();

		LOG.info("Opening StreamSortOperator");
		ExecutionConfig executionConfig = getExecutionConfig();
		this.baseRowSerializer = inputRowType.createSerializer(executionConfig);
		this.binarySerializer = new BinaryRowSerializer(baseRowSerializer.getArity());

		ClassLoader cl = getContainingTask().getUserCodeClassLoader();
		NormalizedKeyComputer computer = gComputer.newInstance(cl);
		RecordComparator comparator = gComparator.newInstance(cl);
		gComputer = null;
		gComparator = null;

		this.sorter = new BinaryExternalSorter(this.getContainingTask(),
				getContainingTask().getEnvironment().getMemoryManager(), memorySize,
				this.getContainingTask().getEnvironment().getIOManager(), baseRowSerializer,
				binarySerializer, computer, comparator, getContainingTask().getJobConfiguration());
		this.sorter.startThreads();

		//register the the metrics.
		getMetricGroup().gauge("memoryUsedSizeInBytes", (Gauge<Long>) sorter::getUsedMemoryInBytes);
		getMetricGroup().gauge("numSpillFiles", (Gauge<Long>) sorter::getNumSpillFiles);
		getMetricGroup().gauge("spillInBytes", (Gauge<Long>) sorter::getSpillInBytes);

		this.collector = new StreamRecordCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<BaseRow> element) throws Exception {
		BaseRow originalInput = element.getValue();
		BinaryRow input = baseRowSerializer.baseRowToBinary(originalInput).copy();
		BaseRowUtil.setAccumulate(input);
		long count = inputBuffer.getOrDefault(input, 0L);
		if (BaseRowUtil.isAccumulateMsg(originalInput)) {
			inputBuffer.put(input, count + 1);
		} else {
			if (count == 0L) {
				throw new RuntimeException("BaseRow not exist!");
			} else if (count == 1) {
				inputBuffer.remove(input);
			} else {
				inputBuffer.put(input, count - 1);
			}
		}
	}

	public void endInput() throws Exception {
		if (!inputBuffer.isEmpty()) {
			inputBuffer.entrySet().forEach((Map.Entry<BaseRow, Long> entry) -> {
				long count = entry.getValue();
				BaseRow row = entry.getKey();
				for (int i = 1; i <= count; i++) {
					try {
						sorter.write(row);
					} catch (IOException e) {
						throw new RuntimeException(e);
					}
				}
			});

			// emit the sorted inputs
			BinaryRow row = binarySerializer.createInstance();
			MutableObjectIterator<BinaryRow> iterator = sorter.getIterator();
			while ((row = iterator.next(row)) != null) {
				collector.collect(row);
			}
		}
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);
		TupleTypeInfo<Tuple2<BaseRow, Long>> tupleType = new TupleTypeInfo<>(inputRowType, Types.LONG);
		this.bufferState = context.getOperatorStateStore()
				.getListState(new ListStateDescriptor<>("localBufferState", tupleType));
		// restore state
		if (context.isRestored()) {
			bufferState.get().forEach((Tuple2<BaseRow, Long> input) -> inputBuffer.put(input.f0, input.f1));
		}
	}

	@Override
	public void snapshotState(StateSnapshotContext context) throws Exception {
		super.snapshotState(context);
		// clear state first
		bufferState.clear();

		List<Tuple2<BaseRow, Long>> dataToFlush = new ArrayList<>(inputBuffer.size());
		inputBuffer.entrySet().forEach(
				(Map.Entry<BaseRow, Long> entry) -> dataToFlush.add(Tuple2.of(entry.getKey(), entry.getValue())));

		// batch put
		bufferState.addAll(dataToFlush);
	}

	@Override
	public void close() throws Exception {
		endInput(); // TODO after introduce endInput

		LOG.info("Closing StreamSortOperator");
		super.close();
		if (sorter != null) {
			sorter.close();
		}
	}
}
