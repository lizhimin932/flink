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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorTest extends TestLogger {
	
	@Test
	public void testExecutionConfigSerialization() throws IOException, ClassNotFoundException {
		final long seed = System.currentTimeMillis();
		final Random r = new Random(seed);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		StreamGraph streamingJob = new StreamGraph(env);
		StreamingJobGraphGenerator compiler = new StreamingJobGraphGenerator(streamingJob);
		
		boolean closureCleanerEnabled = r.nextBoolean(), forceAvroEnabled = r.nextBoolean(), forceKryoEnabled = r.nextBoolean(), objectReuseEnabled = r.nextBoolean(), sysoutLoggingEnabled = r.nextBoolean();
		int dop = 1 + r.nextInt(10);
		
		ExecutionConfig config = streamingJob.getExecutionConfig();
		if(closureCleanerEnabled) {
			config.enableClosureCleaner();
		} else {
			config.disableClosureCleaner();
		}
		if(forceAvroEnabled) {
			config.enableForceAvro();
		} else {
			config.disableForceAvro();
		}
		if(forceKryoEnabled) {
			config.enableForceKryo();
		} else {
			config.disableForceKryo();
		}
		if(objectReuseEnabled) {
			config.enableObjectReuse();
		} else {
			config.disableObjectReuse();
		}
		if(sysoutLoggingEnabled) {
			config.enableSysoutLogging();
		} else {
			config.disableSysoutLogging();
		}
		config.setParallelism(dop);
		
		JobGraph jobGraph = compiler.createJobGraph();

		final String EXEC_CONFIG_KEY = "runtime.config";

		InstantiationUtil.writeObjectToConfig(jobGraph.getSerializedExecutionConfig(),
			jobGraph.getJobConfiguration(),
			EXEC_CONFIG_KEY);

		SerializedValue<ExecutionConfig> serializedExecutionConfig = InstantiationUtil.readObjectFromConfig(
				jobGraph.getJobConfiguration(),
				EXEC_CONFIG_KEY,
				Thread.currentThread().getContextClassLoader());

		assertNotNull(serializedExecutionConfig);

		ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(getClass().getClassLoader());

		assertEquals(closureCleanerEnabled, executionConfig.isClosureCleanerEnabled());
		assertEquals(forceAvroEnabled, executionConfig.isForceAvroEnabled());
		assertEquals(forceKryoEnabled, executionConfig.isForceKryoEnabled());
		assertEquals(objectReuseEnabled, executionConfig.isObjectReuseEnabled());
		assertEquals(sysoutLoggingEnabled, executionConfig.isSysoutLoggingEnabled());
		assertEquals(dop, executionConfig.getParallelism());
	}
	
	@Test
	public void testParallelismOneNotChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
				.fromElements("a", "b", "c", "d", "e", "f")
				.map(new MapFunction<String, Tuple2<String, String>>() {
					private static final long serialVersionUID = 471891682418382583L;

					@Override
					public Tuple2<String, String> map(String value) {
						return new Tuple2<>(value, value);
					}
				});

		DataStream<Tuple2<String, String>> result = input
				.keyBy(0)
				.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

					private static final long serialVersionUID = 3583760206245136188L;

					@Override
					public Tuple2<String, String> map(Tuple2<String, String> value) {
						return value;
					}
				});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {
			private static final long serialVersionUID = -5614849094269539342L;

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		
		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, verticesSorted.get(0).getParallelism());
		assertEquals(1, verticesSorted.get(1).getParallelism());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapSinkVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapSinkVertex.getInputs().get(0).getSource().getResultType());
	}

	/**
	 * Tests that disabled checkpointing sets the checkpointing interval to Long.MAX_VALUE.
	 */
	@Test
	public void testDisabledCheckpointing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamGraph streamGraph = new StreamGraph(env);
		assertFalse("Checkpointing enabled", streamGraph.getCheckpointConfig().isCheckpointingEnabled());

		StreamingJobGraphGenerator jobGraphGenerator = new StreamingJobGraphGenerator(streamGraph);
		JobGraph jobGraph = jobGraphGenerator.createJobGraph();

		JobSnapshottingSettings snapshottingSettings = jobGraph.getSnapshotSettings();
		assertEquals(Long.MAX_VALUE, snapshottingSettings.getCheckpointInterval());
	}

	/**
	 * Verifies that the chain start/end is correctly set.
	 */
	@Test
	public void testChainStartEndSetting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> CHAIN(Map -> Print)
		env.fromElements(1, 2, 3)
			.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value;
				}
			})
			.print();
		JobGraph jobGraph = new StreamingJobGraphGenerator(env.getStreamGraph()).createJobGraph();

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapPrintVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED_BOUNDED, mapPrintVertex.getInputs().get(0).getSource().getResultType());

		StreamConfig sourceConfig = new StreamConfig(sourceVertex.getConfiguration());
		StreamConfig mapConfig = new StreamConfig(mapPrintVertex.getConfiguration());
		Map<Integer, StreamConfig> chainedConfigs = mapConfig.getTransitiveChainedTaskConfigs(getClass().getClassLoader());
		StreamConfig printConfig = chainedConfigs.values().iterator().next();

		assertTrue(sourceConfig.isChainStart());
		assertTrue(sourceConfig.isChainEnd());

		assertTrue(mapConfig.isChainStart());
		assertFalse(mapConfig.isChainEnd());

		assertFalse(printConfig.isChainStart());
		assertTrue(printConfig.isChainEnd());
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators when
	 * generating job graph
	 */
	@Test
	public void testChainedResourceMerging() throws Exception {
		ResourceSpec resource1 = new ResourceSpec(0.1, 100);
		ResourceSpec resource2 = new ResourceSpec(0.2, 200);
		ResourceSpec resource3 = new ResourceSpec(0.3, 300);
		ResourceSpec resource4 = new ResourceSpec(0.4, 400);
		ResourceSpec resource5 = new ResourceSpec(0.5, 500);
		ResourceSpec resource6 = new ResourceSpec(0.6, 600);

		Method opMethod = SingleOutputStreamOperator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataStreamSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Boolean> source = env.addSource(new ParallelSourceFunction<Boolean>() {
			@Override
			public void run(SourceContext<Boolean> ctx) throws Exception {}

			@Override
			public void cancel() {}
		});
		opMethod.invoke(source, resource1);

		// CHAIN(Source -> Map)
		DataStream<Boolean> map = source.map(new MapFunction<Boolean, Boolean>() {
			@Override
			public Boolean map(Boolean value) throws Exception {
				return value;
			}
		});
		opMethod.invoke(map, resource2);

		IterativeStream<Boolean> iteration = map.iterate(3000);
		opMethod.invoke(iteration, resource3);

		DataStream<Boolean> flatMap = iteration.flatMap(new FlatMapFunction<Boolean, Boolean>() {
			@Override
			public void flatMap(Boolean value, Collector<Boolean> out) throws Exception {}
		});
		opMethod.invoke(flatMap, resource4);

		DataStream<Boolean> increment = flatMap.filter(new FilterFunction<Boolean>() {
			@Override
			public boolean filter(Boolean value) throws Exception {
				return false;
			}
		});
		opMethod.invoke(increment, resource5);

		// CHAIN(Flat Map -> Filter -> Sink)
		DataStreamSink<Boolean> sink = iteration.closeWith(increment).addSink(new SinkFunction<Boolean>() {
			@Override
			public void invoke(Boolean value) {
			}
		});
		sinkMethod.invoke(sink, resource6);

		JobGraph jobGraph = new StreamingJobGraphGenerator(env.getStreamGraph()).createJobGraph();

		JobVertex sourceMapVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
		JobVertex iterationHeadVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
		JobVertex flatMapFilterSinkVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(2);
		JobVertex iterationTailVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(3);

		assertTrue(sourceMapVertex.getMinResources().equals(resource1.merge(resource2)));
		assertTrue(iterationHeadVertex.getPreferredResources().equals(resource3));
		assertTrue(flatMapFilterSinkVertex.getMinResources().equals(resource4.merge(resource5).merge(resource6)));
		// the iteration tail task will be scheduled in the same instance with iteration head, and currently not set resources.
		assertTrue(iterationTailVertex.getPreferredResources().equals(ResourceSpec.DEFAULT));
	}
}
