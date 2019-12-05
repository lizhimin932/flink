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

package org.apache.flink.runtime.rest.handler.util;

import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.handler.job.JobVertexDetailsHandler;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;

import javax.annotation.Nullable;

/**
 * This class is a mutable version of the {@link IOMetrics} class that allows adding up IO-related metrics.
 *
 * <p>For finished jobs these metrics are stored in the {@link ExecutionGraph} as another {@link IOMetrics}.
 * For running jobs these metrics are retrieved using the {@link MetricFetcher}.
 *
 * <p>This class provides a common interface to handle both cases, reducing complexity in various handlers (like
 * the {@link JobVertexDetailsHandler}).
 */
public class MutableIOMetrics extends IOMetrics {

	private static final long serialVersionUID = -5460777634971381737L;
	private boolean numBytesInComplete = true;
	private boolean numBytesOutComplete = true;
	private boolean numRecordsInComplete = true;
	private boolean numRecordsOutComplete = true;
	private boolean usageInputFloatingBuffersComplete = true;
	private boolean usageInputExclusiveBuffersComplete = true;
	private boolean usageOutPoolComplete = true;
	private boolean isBackPressuredComplete = true;

	public MutableIOMetrics() {
		super(0, 0, 0, 0, 0.0f, 0.0f, 0.0f, false);
	}

	public boolean isNumBytesInComplete() {
		return numBytesInComplete;
	}

	public boolean isNumBytesOutComplete() {
		return numBytesOutComplete;
	}

	public boolean isNumRecordsInComplete() {
		return numRecordsInComplete;
	}

	public boolean isNumRecordsOutComplete() {
		return numRecordsOutComplete;
	}

	public boolean isUsageInputFloatingBuffersComplete() {
		return usageInputFloatingBuffersComplete;
	}

	public boolean isUsageInputExclusiveBuffersComplete() {
		return usageInputExclusiveBuffersComplete;
	}

	public boolean isUsageOutPoolComplete() {
		return usageOutPoolComplete;
	}

	public boolean isBackPressuredComplete() {
		return isBackPressuredComplete;
	}

	/**
	 * Adds the IO metrics for the given attempt to this object. If the {@link AccessExecution} is in
	 * a terminal state the contained {@link IOMetrics} object is added. Otherwise the given {@link MetricFetcher} is
	 * used to retrieve the required metrics.
	 *
	 * @param attempt Attempt whose IO metrics should be added
	 * @param fetcher MetricFetcher to retrieve metrics for running jobs
	 * @param jobID JobID to which the attempt belongs
	 * @param taskID TaskID to which the attempt belongs
	 */
	public void addIOMetrics(AccessExecution attempt, @Nullable MetricFetcher fetcher, String jobID, String taskID) {
		if (attempt.getState().isTerminal()) {
			IOMetrics ioMetrics = attempt.getIOMetrics();
			if (ioMetrics != null) { // execAttempt is already finished, use final metrics stored in ExecutionGraph
				this.numBytesIn += ioMetrics.getNumBytesIn();
				this.numBytesOut += ioMetrics.getNumBytesOut();
				this.numRecordsIn += ioMetrics.getNumRecordsIn();
				this.numRecordsOut += ioMetrics.getNumRecordsOut();
				this.usageInputExclusiveBuffers += ioMetrics.getUsageInputExclusiveBuffers();
				this.usageInputFloatingBuffers += ioMetrics.getUsageInputFloatingBuffers();
				this.usageOutPool += ioMetrics.getUsageOutPool();
				this.isBackPressured &= ioMetrics.isBackPressured();
			}
		} else { // execAttempt is still running, use MetricQueryService instead
			if (fetcher != null) {
				fetcher.update();
				MetricStore.ComponentMetricStore metrics = fetcher.getMetricStore()
					.getSubtaskMetricStore(jobID, taskID, attempt.getParallelSubtaskIndex());
				if (metrics != null) {
					/**
					 * We want to keep track of missing metrics to be able to make a difference between 0 as a value
					 * and a missing value.
					 * In case a metric is missing for a parallel instance of a task, we set the complete flag as
					 * false.
					 */
					updateLong(metrics, MetricNames.IO_NUM_BYTES_IN,
						(String value) -> this.numBytesInComplete = false,
						(Long value) -> this.numBytesIn += value
					);

					updateLong(metrics, MetricNames.IO_NUM_BYTES_OUT,
						(String value) -> this.numBytesOutComplete = false,
						(Long value) -> this.numBytesOut += value
					);

					updateLong(metrics, MetricNames.IO_NUM_RECORDS_IN,
						(String value) -> this.numRecordsInComplete = false,
						(Long value) -> this.numRecordsIn += value
					);

					updateLong(metrics, MetricNames.IO_NUM_RECORDS_OUT,
						(String value) -> this.numRecordsOutComplete = false,
						(Long value) -> this.numRecordsOut += value
					);

					updateFloat(metrics, MetricNames.IO_NUM_RECORDS_OUT,
						(String value) -> this.numRecordsOutComplete = false,
						(Float value) -> this.numRecordsOut += value
					);
				}
				else {
					this.numBytesInComplete = false;
					this.numBytesOutComplete = false;
					this.numRecordsInComplete = false;
					this.numRecordsOutComplete = false;
				}
			}
		}
	}

	private void updateLong(MetricStore.ComponentMetricStore metrics, String metricKey, Consumer<String> emptyFunction, Consumer<Long> noEmptyFunction) {
		String value = metrics.getMetric(metricKey);
		if (value == null){
			emptyFunction.accept(value);
		}
		else {
			noEmptyFunction.accept(Long.valueOf(value));
		}
	}

	private void updateFloat(MetricStore.ComponentMetricStore metrics, String metricKey, Consumer<String> emptyFunction, Consumer<Float> noEmptyFunction) {
		String value = metrics.getMetric(metricKey);
		if (value == null){
			emptyFunction.accept(value);
		}
		else {
			noEmptyFunction.accept(Float.valueOf(value));
		}
	}

	private void updateBoolean(MetricStore.ComponentMetricStore metrics, String metricKey, Consumer<String> emptyFunction, Consumer<Boolean> noEmptyFunction) {
		String value = metrics.getMetric(metricKey);
		if (value == null){
			emptyFunction.accept(value);
		}
		else {
			noEmptyFunction.accept(Boolean.valueOf(value));
		}
	}
}
