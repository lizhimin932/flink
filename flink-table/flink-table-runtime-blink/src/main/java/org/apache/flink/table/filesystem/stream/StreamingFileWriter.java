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

package org.apache.flink.table.filesystem.stream;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;

/**
 * Writer for emitting {@link PartitionCommitInfo} to downstream.
 */
public class StreamingFileWriter<IN> extends AbstractStreamingWriter<IN, PartitionCommitInfo> {

	private static final long serialVersionUID = 2L;

	private transient Set<String> inactivePartitions;

	public StreamingFileWriter(
			long bucketCheckInterval,
			StreamingFileSink.BucketsBuilder<IN, String, ? extends
					StreamingFileSink.BucketsBuilder<IN, String, ?>> bucketsBuilder) {
		super(bucketCheckInterval, bucketsBuilder);
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		inactivePartitions = new HashSet<>();
		super.initializeState(context);
	}

	@Override
	protected void partitionInactive(String partition) {
		inactivePartitions.add(partition);
	}

	@Override
	protected void commitUpToCheckpoint(long checkpointId) throws Exception {
		super.commitUpToCheckpoint(checkpointId);
		output.collect(new StreamRecord<>(new PartitionCommitInfo(
				checkpointId,
				getRuntimeContext().getIndexOfThisSubtask(),
				getRuntimeContext().getNumberOfParallelSubtasks(),
				new ArrayList<>(inactivePartitions))));
		inactivePartitions.clear();
	}
}
