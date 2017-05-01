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
package org.apache.flink.streaming.api.windowing.windows;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeutils.ParameterlessTypeSerializerConfig;
import org.apache.flink.api.common.typeutils.ReconfigureResult;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.util.MathUtils;

/**
 * A {@link Window} that represents a time interval from {@code start} (inclusive) to
 * {@code end} (exclusive).
 */
@PublicEvolving
public class TimeWindow extends Window {

	private final long start;
	private final long end;

	public TimeWindow(long start, long end) {
		this.start = start;
		this.end = end;
	}

	/**
	 * Gets the starting timestamp of the window. This is the first timestamp that belongs
	 * to this window.
	 *
	 * @return The starting timestamp of this window.
	 */
	public long getStart() {
		return start;
	}

	/**
	 * Gets the end timestamp of this window. The end timestamp is exclusive, meaning it
	 * is the first timestamp that does not belong to this window any more.
	 *
	 * @return The exclusive end timestamp of this window.
	 */
	public long getEnd() {
		return end;
	}

	/**
	 * Gets the largest timestamp that still belongs to this window.
	 *
	 * <p>This timestamp is identical to {@code getEnd() - 1}.
	 *
	 * @return The largest timestamp that still belongs to this window.
	 *
	 * @see #getEnd()
	 */
	@Override
	public long maxTimestamp() {
		return end - 1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		TimeWindow window = (TimeWindow) o;

		return end == window.end && start == window.start;
	}

	@Override
	public int hashCode() {
		return MathUtils.longToIntWithBitMixing(start + end);
	}

	@Override
	public String toString() {
		return "TimeWindow{" +
				"start=" + start +
				", end=" + end +
				'}';
	}

	/**
	 * Returns {@code true} if this window intersects the given window.
	 */
	public boolean intersects(TimeWindow other) {
		return this.start <= other.end && this.end >= other.start;
	}

	/**
	 * Returns the minimal window covers both this window and the given window.
	 */
	public TimeWindow cover(TimeWindow other) {
		return new TimeWindow(Math.min(start, other.start), Math.max(end, other.end));
	}

	// ------------------------------------------------------------------------
	// Serializer
	// ------------------------------------------------------------------------

	/**
	 * The serializer used to write the TimeWindow type.
	 */
	public static class Serializer extends TypeSerializer<TimeWindow> {
		private static final long serialVersionUID = 1L;

		private static final TimeWindowSerializerConfig CONFIG = new TimeWindowSerializerConfig();

		@Override
		public boolean isImmutableType() {
			return true;
		}

		@Override
		public TypeSerializer<TimeWindow> duplicate() {
			return this;
		}

		@Override
		public TimeWindow createInstance() {
			return null;
		}

		@Override
		public TimeWindow copy(TimeWindow from) {
			return from;
		}

		@Override
		public TimeWindow copy(TimeWindow from, TimeWindow reuse) {
			return from;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(TimeWindow record, DataOutputView target) throws IOException {
			target.writeLong(record.start);
			target.writeLong(record.end);
		}

		@Override
		public TimeWindow deserialize(DataInputView source) throws IOException {
			long start = source.readLong();
			long end = source.readLong();
			return new TimeWindow(start, end);
		}

		@Override
		public TimeWindow deserialize(TimeWindow reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			target.writeLong(source.readLong());
			target.writeLong(source.readLong());
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof Serializer;
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof Serializer;
		}

		@Override
		public int hashCode() {
			return 0;
		}

		// --------------------------------------------------------------------------------------------
		// Serializer configuration snapshotting & reconfiguring
		// --------------------------------------------------------------------------------------------

		@Override
		public TimeWindowSerializerConfig snapshotConfiguration() {
			return CONFIG;
		}

		@Override
		public ReconfigureResult reconfigure(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof TimeWindowSerializerConfig) {
				return ReconfigureResult.COMPATIBLE;
			} else {
				return ReconfigureResult.INCOMPATIBLE;
			}
		}
	}

	/**
	 * A {@link TypeSerializerConfigSnapshot} specific to the {@link TimeWindow} serializer.
	 */
	public static final class TimeWindowSerializerConfig extends ParameterlessTypeSerializerConfig {}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	/**
	 * Merge overlapping {@link TimeWindow}s. For use by merging
	 * {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner WindowAssigners}.
	 */
	public static void mergeWindows(Collection<TimeWindow> windows, MergingWindowAssigner.MergeCallback<TimeWindow> c) {

		// sort the windows by the start time and then merge overlapping windows

		List<TimeWindow> sortedWindows = new ArrayList<>(windows);

		Collections.sort(sortedWindows, new Comparator<TimeWindow>() {
			@Override
			public int compare(TimeWindow o1, TimeWindow o2) {
				return Long.compare(o1.getStart(), o2.getStart());
			}
		});

		List<Tuple2<TimeWindow, Set<TimeWindow>>> merged = new ArrayList<>();
		Tuple2<TimeWindow, Set<TimeWindow>> currentMerge = null;

		for (TimeWindow candidate: sortedWindows) {
			if (currentMerge == null) {
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			} else if (currentMerge.f0.intersects(candidate)) {
				currentMerge.f0 = currentMerge.f0.cover(candidate);
				currentMerge.f1.add(candidate);
			} else {
				merged.add(currentMerge);
				currentMerge = new Tuple2<>();
				currentMerge.f0 = candidate;
				currentMerge.f1 = new HashSet<>();
				currentMerge.f1.add(candidate);
			}
		}

		if (currentMerge != null) {
			merged.add(currentMerge);
		}

		for (Tuple2<TimeWindow, Set<TimeWindow>> m: merged) {
			if (m.f1.size() > 1) {
				c.merge(m.f1, m.f0);
			}
		}
	}

	/**
	 * Method to get the window start for a timestamp.
	 *
	 * @param timestamp epoch millisecond to get the window start.
	 * @param offset The offset which window start would be shifted by.
	 * @param windowSize The size of the generated windows.
	 * @return window start
	 */
	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
	}
}
