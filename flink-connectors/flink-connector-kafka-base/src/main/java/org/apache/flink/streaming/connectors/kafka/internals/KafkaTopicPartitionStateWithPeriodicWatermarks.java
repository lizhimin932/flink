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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

/**
 * A special version of the per-kafka-partition-state that additionally holds
 * a periodic watermark generator (and timestamp extractor) per partition.
 *
 * @param <T> The type of records handled by the watermark generator
 * @param <KPH> The type of the Kafka partition descriptor, which varies across Kafka versions.
 */
@Internal
public final class KafkaTopicPartitionStateWithPeriodicWatermarks<T, KPH> extends KafkaTopicPartitionState<KPH> {

	/** The timestamp assigner and watermark generator for the partition. */
	private final AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks;

	/** The last watermark timestamp generated by this partition. */
	private long partitionWatermark;

	/** Last processing time a record was received. */
	private long lastRecordProcessingTime = Long.MIN_VALUE;

	/** The configured partition idle timeout. */
	private final long partitionIdleTimeout;

	/** The maximum extracted timestamp so far; used to determine whether or not we expect a newer watermark. */
	private long maxExtractedTimestamp;

	/** Flag indicating whether or not we expect a newer watermark. */
	private boolean hasNewWatermark;

	/** Flag indicating whether or not this partition's watermark is already the overall minimum watermark. */
	private boolean watermarkAlreadyProcessed;

	// ------------------------------------------------------------------------

	public KafkaTopicPartitionStateWithPeriodicWatermarks(
			KafkaTopicPartition partition, KPH kafkaPartitionHandle,
			AssignerWithPeriodicWatermarks<T> timestampsAndWatermarks,
			long partitionIdleTimeout) {
		super(partition, kafkaPartitionHandle);

		this.timestampsAndWatermarks = timestampsAndWatermarks;
		this.partitionWatermark = Long.MIN_VALUE;
		this.partitionIdleTimeout = partitionIdleTimeout;
	}

	// ------------------------------------------------------------------------

	public void setlastRecordProcessingTime(long currentProcessingTimestamp) {
		lastRecordProcessingTime = currentProcessingTimestamp;
	}

	public long getTimestampForRecord(T record, long kafkaEventTimestamp) {
		long extractTimestamp = timestampsAndWatermarks.extractTimestamp(record, kafkaEventTimestamp);

		// if newer timestamp is extracted, the partition can't be idle yet (a watermark based on this new record should be created first)
		if (extractTimestamp > maxExtractedTimestamp) {
			maxExtractedTimestamp = extractTimestamp;
			hasNewWatermark = true;
		}

		return extractTimestamp;
	}

	public long getCurrentWatermarkTimestamp() {
		Watermark wm = timestampsAndWatermarks.getCurrentWatermark();
		if (wm != null) {
			partitionWatermark = Math.max(partitionWatermark, wm.getTimestamp());
		}

		// since we now return the latest watermark, this flag can be cleared
		hasNewWatermark = false;

		return partitionWatermark;
	}

	// ------------------------------------------------------------------------

	public boolean isIdle(long currentProcessingTimestamp) {
		return partitionIdleTimeout != FlinkKafkaConsumerBase.PARTITION_IDLE_DISABLED
				&& !hasNewWatermark
				&& watermarkAlreadyProcessed
				&& lastRecordProcessingTime <= currentProcessingTimestamp - partitionIdleTimeout;
	}

	public void notifyMinimalWatermark(long minWatermark) {
		watermarkAlreadyProcessed = minWatermark >= partitionWatermark;
	}

	@Override
	public String toString() {
		return "KafkaTopicPartitionStateWithPeriodicWatermarks: partition=" + getKafkaTopicPartition()
				+ ", offset=" + getOffset() + ", watermark=" + partitionWatermark;
	}
}
