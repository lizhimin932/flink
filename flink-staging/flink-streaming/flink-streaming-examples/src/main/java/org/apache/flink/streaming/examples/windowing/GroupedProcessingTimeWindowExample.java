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

package org.apache.flink.streaming.examples.windowing;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.windows.KeyedWindowFunction;
import org.apache.flink.streaming.runtime.operators.windows.AggregatingProcessingTimeWindowOperator;
import org.apache.flink.util.Collector;

@SuppressWarnings("serial")
public class GroupedProcessingTimeWindowExample {
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(4);
		
		DataStream<Tuple2<Long, Long>> stream = env
				.addSource(new RichParallelSourceFunction<Tuple2<Long, Long>>() {
					
					private volatile boolean running = true;
					
					@Override
					public void run(SourceContext<Tuple2<Long, Long>> ctx) throws Exception {
						
						final long startTime = System.currentTimeMillis();
						
						final long numElements = 20000000;
						final long numKeys = 10000;
						long val = 1L;
						long count = 0L;
						
						
						while (running && count < numElements) {
							count++;
							ctx.collect(new Tuple2<Long, Long>(val++, 1L));
							
							if (val > numKeys) {
								val = 1L;
							}
						}

						final long endTime = System.currentTimeMillis();
						System.out.println("Took " + (endTime-startTime) + " msecs for " + numElements + " values");
					}

					@Override
					public void cancel() {
						running = false;
					}
				});
		
		stream
//				.groupBy(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
//				.window(Time.of(2500, TimeUnit.MILLISECONDS)).every(Time.of(500, TimeUnit.MILLISECONDS))
//				.reduceWindow(new SummingReducer())
//				.flatten()
//		.partitionByHash(new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>())
//		.transform(
//				"Aligned time window",
//				TypeInfoParser.<Tuple2<Long, Long>>parse("Tuple2<Long, Long>"),
//				new AccumulatingProcessingTimeWindowOperator<Long, Tuple2<Long, Long>, Tuple2<Long, Long>>(
//						new SummingWindowFunction<Long>(),
//						new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>(),
//						2500, 500))
			.transform(
				"Aligned time window",
				TypeInfoParser.<Tuple2<Long, Long>>parse("Tuple2<Long, Long>"),
				new AggregatingProcessingTimeWindowOperator<Long, Tuple2<Long, Long>>(
						new SummingReducer(),
						new FirstFieldKeyExtractor<Tuple2<Long, Long>, Long>(),
						2500, 500))
				
			.addSink(new SinkFunction<Tuple2<Long, Long>>() {
					@Override
					public void invoke(Tuple2<Long, Long> value) {
			}
		});
		
		env.execute();
	}
	
	public static class FirstFieldKeyExtractor<Type extends Tuple, Key> implements KeySelector<Type, Key> {
		
		@Override
		@SuppressWarnings("unchecked")
		public Key getKey(Type value) {
			return (Key) value.getField(0);
		}
	}

	public static class IdentityKeyExtractor<T> implements KeySelector<T, T> {

		@Override
		public T getKey(T value) {
			return value;
		}
	}

	public static class IdentityWindowFunction<K, T> implements KeyedWindowFunction<K, T, T> {

		@Override
		public void evaluate(K k, Iterable<T> values, Collector<T> out) throws Exception {
			for (T v : values) {
				out.collect(v);
			}
		}
	}
	
	public static class CountingWindowFunction<K, T> implements KeyedWindowFunction<K, T, Long> {
		
		@Override
		public void evaluate(K k, Iterable<T> values, Collector<Long> out) throws Exception {
			long count = 0;
			for (T ignored : values) {
				count++;
			}

			out.collect(count);
		}
	}

	public static class SummingWindowFunction<K> implements KeyedWindowFunction<K, Tuple2<K, Long>, Tuple2<K, Long>> {

		@Override
		public void evaluate(K key, Iterable<Tuple2<K, Long>> values, Collector<Tuple2<K, Long>> out) throws Exception {
			long sum = 0L;
			for (Tuple2<K, Long> value : values) {
				sum += value.f1;
			}

			out.collect(new Tuple2<K, Long>(key, sum));
		}
	}

	public static class SummingReducer implements ReduceFunction<Tuple2<Long, Long>> {

		@Override
		public Tuple2<Long, Long> reduce(Tuple2<Long, Long> value1, Tuple2<Long, Long> value2) {
			return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
		}
	}
}
