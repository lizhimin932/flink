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

package org.apache.flink.streaming.connectors.elasticsearch;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.ElasticsearchResource;
import org.apache.flink.streaming.connectors.elasticsearch.testutils.SourceSinkDataTestKit;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.runtime.utils.TestingAppendRowDataSink;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.test.util.AbstractTestBase;

import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.junit.ClassRule;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Environment preparation and suite of tests for version-specific {@link ElasticsearchSinkBase} implementations.
 *
 * @param <C> Elasticsearch client type
 * @param <A> The address type to use
 */
public abstract class ElasticsearchSinkTestBase<T, C extends AutoCloseable, A> extends AbstractTestBase {

	protected static final String CLUSTER_NAME = "test-cluster";

	@ClassRule
	public static ElasticsearchResource elasticsearchResource = new ElasticsearchResource(CLUSTER_NAME);

	/**
	 * Tests that the Elasticsearch sink works properly with json.
	 */
	public void runElasticsearchSinkTest() throws Exception {
		runElasticSearchSinkTest("elasticsearch-sink-test-json-index", SourceSinkDataTestKit::getJsonSinkFunction);
	}

	/**
	 * Tests that the Elasticsearch sink works properly with cbor.
	 */
	public void runElasticsearchSinkCborTest() throws Exception {
		runElasticSearchSinkTest("elasticsearch-sink-test-cbor-index", SourceSinkDataTestKit::getCborSinkFunction);
	}

	/**
	 * Tests that the Elasticsearch sink works properly with smile.
	 */
	public void runElasticsearchSinkSmileTest() throws Exception {
		runElasticSearchSinkTest("elasticsearch-sink-test-smile-index", SourceSinkDataTestKit::getSmileSinkFunction);
	}

	/**
	 * Tests that the Elasticsearch sink works properly with yaml.
	 */
	public void runElasticsearchSinkYamlTest() throws Exception {
		runElasticSearchSinkTest("elasticsearch-sink-test-yaml-index", SourceSinkDataTestKit::getYamlSinkFunction);
	}

	private void runElasticSearchSinkTest(String index, Function<String, ElasticsearchSinkFunction<Tuple2<Integer, String>>> functionFactory) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		source.addSink(createElasticsearchSinkForEmbeddedNode(
			1,
			CLUSTER_NAME,
			functionFactory.apply(index)));

		env.execute("Elasticsearch Sink Test");

		// verify the results
		Client client = elasticsearchResource.getClient();
		SourceSinkDataTestKit.verifyProducedSinkData(client, index);

		client.close();
	}

	protected void runElasticSearchInputFormatTest() throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", CLUSTER_NAME);
		DataType dataType = ROW(FIELD("data", STRING()));
		RowType schema = (RowType) dataType.getLogicalType();

		// pass on missing field
		DeserializationSchema<RowData> deserializationSchema = new JsonRowDataDeserializationSchema(
			schema, new RowDataTypeInfo(schema), false, false, TimestampFormat.ISO_8601);

		ElasticSearchInputFormatBase inputFormat = createElasticsearchInputFormat(
			userConfig,
			(DeserializationSchema<T>) deserializationSchema,
			null,
			"elasticsearch-sink-test-json-index",
			"flink-es-test-type",
			1000,
			10,
			null,
			0
		);

		DataStream<RowData> dataStream = env.createInput(inputFormat);
		TestingAppendRowDataSink sink = new TestingAppendRowDataSink(new RowDataTypeInfo(schema));
		dataStream.addSink(sink);
		env.execute("Elasticsearch Source Test");
		List<String> expected = Arrays.asList(
			"+I(message #0)",
			"+I(message #1)",
			"+I(message #10)",
			"+I(message #11)",
			"+I(message #12)",
			"+I(message #13)",
			"+I(message #14)",
			"+I(message #15)",
			"+I(message #16)",
			"+I(message #17)",
			"+I(message #18)",
			"+I(message #19)",
			"+I(message #2)",
			"+I(message #3)",
			"+I(message #4)",
			"+I(message #5)",
			"+I(message #6)",
			"+I(message #7)",
			"+I(message #8)",
			"+I(message #9)"
		);
		List<String> results = sink.getJavaAppendResults();
		results.sort(String::compareTo);
		assertEquals(expected, results);
	}

	protected abstract ElasticSearchInputFormatBase createElasticsearchInputFormat(
		Map<String, String> userConfig,
		DeserializationSchema<T> deserializationSchema,
		String[] fieldNames,
		String index,
		String type,
		long scrollTimeout,
		int scrollMaxSize,
		QueryBuilder predicate,
		int limit
	) throws Exception;

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of addresses is {@code null}.
	 */
	public void runNullAddressesTest() throws Exception {
		try {
			createElasticsearchSink(
				1,
				CLUSTER_NAME,
				null,
				SourceSinkDataTestKit.getJsonSinkFunction("test"));
		} catch (IllegalArgumentException | NullPointerException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests that the Elasticsearch sink fails eagerly if the provided list of addresses is empty.
	 */
	public void runEmptyAddressesTest() throws Exception {
		try {
			createElasticsearchSink(
				1,
				CLUSTER_NAME,
				Collections.emptyList(),
				SourceSinkDataTestKit.getJsonSinkFunction("test"));
		} catch (IllegalArgumentException expectedException) {
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Tests whether the Elasticsearch sink fails when there is no cluster to connect to.
	 */
	public void runInvalidElasticsearchClusterTest() throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStreamSource<Tuple2<Integer, String>> source = env.addSource(new SourceSinkDataTestKit.TestDataSourceFunction());

		source.addSink(createElasticsearchSinkForNode(
			1,
			"invalid-cluster-name",
			SourceSinkDataTestKit.getJsonSinkFunction("test"),
			"123.123.123.123")); // incorrect ip address

		try {
			env.execute("Elasticsearch Sink Test");
		} catch (JobExecutionException expectedException) {
			// every ES version throws a different exception in case of timeouts, so don't bother asserting on the exception
			// test passes
			return;
		}

		fail();
	}

	/**
	 * Utility method to create a user config map.
	 */
	protected Map<String, String> createUserConfig(int bulkFlushMaxActions, String clusterName) {
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", clusterName);
		userConfig.put(ElasticsearchSinkBase.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(bulkFlushMaxActions));
		userConfig.put("transport.tcp.connect_timeout", "5s");

		return userConfig;
	}

	/**
	 * Creates a version-specific Elasticsearch sink, using arbitrary transport addresses.
	 */
	protected abstract ElasticsearchSinkBase<Tuple2<Integer, String>, C> createElasticsearchSink(
		int bulkFlushMaxActions,
		String clusterName,
		List<A> addresses,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction);

	/**
	 * Creates a version-specific Elasticsearch sink to connect to a local embedded Elasticsearch node.
	 *
	 * <p>This case is singled out from {@link ElasticsearchSinkTestBase#createElasticsearchSink(int, String, List, ElasticsearchSinkFunction)}
	 * because the Elasticsearch Java API to do so is incompatible across different versions.
	 */
	protected abstract ElasticsearchSinkBase<Tuple2<Integer, String>, C> createElasticsearchSinkForEmbeddedNode(
		int bulkFlushMaxActions,
		String clusterName,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction) throws Exception;

	/**
	 * Creates a version-specific Elasticsearch sink to connect to a specific Elasticsearch node.
	 */
	protected abstract ElasticsearchSinkBase<Tuple2<Integer, String>, C> createElasticsearchSinkForNode(
		int bulkFlushMaxActions,
		String clusterName,
		ElasticsearchSinkFunction<Tuple2<Integer, String>> elasticsearchSinkFunction,
		String ipAddress) throws Exception;
}
