/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.co;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.Serializable;

/**
 * A function that processes two joined elements and produces a single output one.
 *
 * <p>This function will get called for every joined pair of elements the joined two streams.
 * The timestamp of the joined pair as well as the timestamp of the left element and the right
 * element can be accessed through the {@link Context}.
 *
 * @param <IN1> Type of the first input
 * @param <IN2> Type of the second input
 * @param <OUT> Type of the output
 */
@PublicEvolving
public abstract class ProcessJoinFunction<IN1, IN2, OUT> extends AbstractRichFunction {

	private static final long serialVersionUID = -2444626938039012398L;

	/**
	 * Parameters used {@code IntervalJoinOperator} to optimize joiner performance.
	 */
	public static class JoinParameters implements Serializable {
		/**
		 * {@code IntervalJoinOperator} cleanup records from {@link IN2} at timestamp + rightSideCleanupOverwrite time.
		 */
		public long rightSideCleanupOverwrite;
		/**
		 * size of one side cache used in {@code IntervalJoinOperator}. Default is 1000 keys.
		 */
		public long maxCachedKeyedBufferEntries;
		/**
		 *  expiration time after cache last time access, measured by low watermark advances.
		 */
		public long cacheExpiresInWatermark;

		public JoinParameters() {
			this(Long.MAX_VALUE, 1000L, Long.MAX_VALUE);
		}

		public JoinParameters(long rightSideCleanupOverwrite, long maxCachedKeyedBufferEntries, long cacheExpiresInWatermark) {
			this.rightSideCleanupOverwrite = rightSideCleanupOverwrite;
			this.maxCachedKeyedBufferEntries = maxCachedKeyedBufferEntries;
			this.cacheExpiresInWatermark = cacheExpiresInWatermark;
		}
	}

	protected JoinParameters joinParameters = new JoinParameters();

	/**
	 * This method called in {@code IntervalJoinOperator} constructor. It can tune join optimizations
	 * implemented.
	 * @return The {@code JoinParameters} passed in {@code IntervalJoinOperator} constructor
	 */
	public JoinParameters getJoinParameters() {
		return joinParameters;
	}

	/**
	 * This method is called for each joined pair of elements. It can output zero or more elements
	 * through the provided {@link Collector} and has access to the timestamps of the joined elements
	 * and the result through the {@link Context}.
	 *
	 * @param left         The left element of the joined pair.
	 * @param right        The right element of the joined pair.
	 * @param ctx          A context that allows querying the timestamps of the left, right and
	 *                     joined pair. In addition, this context allows to emit elements on a side output.
	 * @param out          The collector to emit resulting elements to.
	 * @throws Exception   This function may throw exceptions which cause the streaming program to
	 * 					   fail and go in recovery mode.
	 */
	public abstract void processElement(IN1 left, IN2 right, Context ctx, Collector<OUT> out) throws Exception;

	/**
	 * The context that is available during an invocation of
	 * {@link #processElement(Object, Object, Context, Collector)}. It gives access to the timestamps of the
	 * left element in the joined pair, the right one, and that of the joined pair. In addition, this context
	 * allows to emit elements on a side output.
	 */
	public abstract class Context {

		/**
		 * @return The timestamp of the left element of a joined pair
		 */
		public abstract long getLeftTimestamp();

		/**
		 * @return The timestamp of the right element of a joined pair
		 */
		public abstract long getRightTimestamp();

		/**
		 * @return The timestamp of the joined pair.
		 */
		public abstract long getTimestamp();

		/**
		 * Emits a record to the side output identified by the {@link OutputTag}.
		 * @param outputTag The output tag that identifies the side output to emit to
		 * @param value The record to emit
		 */
		public abstract <X> void output(OutputTag<X> outputTag, X value);
	}
}
