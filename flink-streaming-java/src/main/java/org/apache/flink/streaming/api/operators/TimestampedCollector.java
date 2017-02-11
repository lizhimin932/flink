/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.OutputTag;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;
import org.apache.flink.util.RichCollector;

/**
 * Wrapper around an {@link Output} for user functions that expect a {@link Collector}.
 * Before giving the {@link TimestampedCollector} to a user function you must set
 * the timestamp that should be attached to emitted elements. Most operators
 * would set the timestamp of the incoming
 * {@link org.apache.flink.streaming.runtime.streamrecord.StreamRecord} here.
 *
 * @param <T> The type of the elements it emits for non sideoutput.
 */
@Internal
public class TimestampedCollector<T> implements RichCollector<T> {
	
	private final Output<StreamRecord<T>> output;

	private final StreamRecord reuse;

	/**
	 * Creates a new {@link TimestampedCollector} that wraps the given {@link Output}.
	 */
	public TimestampedCollector(Output<StreamRecord<T>> output) {
		this.output = output;
		this.reuse = new StreamRecord<T>(null);
	}

	@Override
	public void collect(T record) {
		output.collect(reuse.replace(record));
	}

	@Override
	public <S> void collect(OutputTag<S> tag, S record) {
		output.collect(reuse.replace(tag, record));
	}

	public void setTimestamp(StreamRecord<?> timestampBase) {
		if (timestampBase.hasTimestamp()) {
			reuse.setTimestamp(timestampBase.getTimestamp());
		} else {
			reuse.eraseTimestamp();
		}
	}

	public void setAbsoluteTimestamp(long timestamp) {
		reuse.setTimestamp(timestamp);
	}

	public void eraseTimestamp() {
		reuse.eraseTimestamp();
	}

	@Override
	public void close() {
		output.close();
	}
}
