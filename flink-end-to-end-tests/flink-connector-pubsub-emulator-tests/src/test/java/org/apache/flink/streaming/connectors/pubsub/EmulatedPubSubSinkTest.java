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

package org.apache.flink.streaming.connectors.pubsub;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pubsub.emulator.GCloudUnitTestBase;
import org.apache.flink.streaming.connectors.pubsub.emulator.PubsubHelper;

import com.google.pubsub.v1.ReceivedMessage;
import org.apache.commons.lang3.StringUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test of the PubSub SINK with the Google PubSub emulator.
 */
public class EmulatedPubSubSinkTest extends GCloudUnitTestBase {
	private static final String PROJECT_NAME = "FLProject";
	private static final String TOPIC_NAME = "FLTopic";
	private static final String SUBSCRIPTION_NAME = "FLSubscription";

	private static PubsubHelper pubsubHelper;

	@BeforeClass
	public static void setUp() throws Exception {
		pubsubHelper = getPubsubHelper();
		pubsubHelper.createTopic(PROJECT_NAME, TOPIC_NAME);
		pubsubHelper.createSubscription(PROJECT_NAME, SUBSCRIPTION_NAME, PROJECT_NAME, TOPIC_NAME);
	}

	@AfterClass
	public static void tearDown() throws Exception {
		pubsubHelper.deleteSubscription(PROJECT_NAME, SUBSCRIPTION_NAME);
		pubsubHelper.deleteTopic(PROJECT_NAME, TOPIC_NAME);
	}

	@Test
	public void testFlinkSink() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		List<String> input = Arrays.asList("One", "Two", "Three", "Four", "Five", "Six", "Seven", "Eigth", "Nine", "Ten");

		// Create test stream
		DataStream<String> theData = env
			.fromCollection(input)
			.name("Test input")
			.map((MapFunction<String, String>) StringUtils::reverse);

		// Sink into pubsub
		theData
			.addSink(PubSubSink.newBuilder(new SimpleStringSchema(), PROJECT_NAME, TOPIC_NAME)
				// Specific for emulator
				.withHostAndPortForEmulator(getPubSubHostPort())
				.build())
			.name("PubSub sink");

		// Run
		env.execute();

		// Now get the result from PubSub and verify if everything is there
		List<ReceivedMessage> receivedMessages = pubsubHelper.pullMessages(PROJECT_NAME, SUBSCRIPTION_NAME, 100);

		assertEquals("Wrong number of elements", input.size(), receivedMessages.size());

		// Check output strings
		List<String> output = new ArrayList<>();
		receivedMessages.forEach(msg -> output.add(msg.getMessage().getData().toStringUtf8()));

		for (String test : input) {
			assertTrue("Missing " + test, output.contains(StringUtils.reverse(test)));
		}
	}

}
