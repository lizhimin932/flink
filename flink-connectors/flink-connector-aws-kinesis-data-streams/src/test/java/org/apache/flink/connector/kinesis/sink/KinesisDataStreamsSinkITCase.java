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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.RandomGenerator;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.core.SdkSystemSetting;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.CreateStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamRequest;
import software.amazon.awssdk.services.kinesis.model.DescribeStreamResponse;
import software.amazon.awssdk.services.kinesis.model.GetRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;
import software.amazon.awssdk.services.kinesis.model.ShardIteratorType;
import software.amazon.awssdk.services.kinesis.model.StreamStatus;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ACCESS_KEY_ID;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_ENDPOINT;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_REGION;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.AWS_SECRET_ACCESS_KEY;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static org.apache.flink.streaming.connectors.kinesis.config.AWSConfigConstants.TRUST_ALL_CERTIFICATES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

/** IT cases for using Kinesis Data Streams Sink based on Kinesalite. */
public class KinesisDataStreamsSinkITCase extends TestLogger {

    private static final String DEFAULT_FIRST_SHARD_NAME = "shardId-000000000000";

    private final ElementConverter<String, PutRecordsRequestEntry> elementConverter =
            KinesisDataStreamsSinkElementConverter.<String>builder()
                    .serializationSchema(new SimpleStringSchema())
                    .partitionKeyGenerator(element -> String.valueOf(element.hashCode()))
                    .build();

    private final ElementConverter<String, PutRecordsRequestEntry>
            partitionKeyTooLongElementConverter =
                    KinesisDataStreamsSinkElementConverter.<String>builder()
                            .serializationSchema(new SimpleStringSchema())
                            .partitionKeyGenerator(element -> element)
                            .build();

    @ClassRule
    public static KinesaliteContainer kinesalite =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE));

    private StreamExecutionEnvironment env;
    private KinesisAsyncClient kinesisClient;

    @Before
    public void setUp() throws Exception {
        System.setProperty(SdkSystemSetting.CBOR_ENABLED.property(), "false");

        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        kinesisClient = kinesalite.getV2Client();
    }

    @Test
    public void elementsMaybeWrittenSuccessfullyToLocalInstanceWhenBatchSizeIsReached()
            throws Exception {
        new Scenario()
                .withKinesaliteStreamName("test-stream-name-1")
                .withSinkConnectionStreamName("test-stream-name-1")
                .runScenario();
    }

    @Test
    public void elementsBufferedAndTriggeredByTimeBasedFlushShouldBeFlushedIfSourcedIsKeptAlive()
            throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(10)
                .withMaxBatchSize(100)
                .withExpectedElements(10)
                .withKinesaliteStreamName("test-stream-name-2")
                .withSinkConnectionStreamName("test-stream-name-2")
                .runScenario();
    }

    @Test
    public void veryLargeMessagesSucceedInBeingPersisted() throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(5)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxSizeBytes(8192)
                .withMaxBatchSize(10)
                .withExpectedElements(5)
                .withKinesaliteStreamName("test-stream-name-3")
                .withSinkConnectionStreamName("test-stream-name-3")
                .runScenario();
    }

    @Test
    public void multipleInFlightRequestsResultsInCorrectNumberOfElementsPersisted()
            throws Exception {
        new Scenario()
                .withNumberOfElementsToSend(150)
                .withSizeOfMessageBytes(2500)
                .withBufferMaxTimeMS(2000)
                .withBufferMaxSizeBytes(8192)
                .withMaxInflightReqs(10)
                .withMaxBatchSize(20)
                .withExpectedElements(150)
                .withKinesaliteStreamName("test-stream-name-4")
                .withSinkConnectionStreamName("test-stream-name-4")
                .runScenario();
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOn() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(true, "test-stream-name-5");
    }

    @Test
    public void nonExistentStreamNameShouldResultInFailureInFailOnErrorIsOff() {
        testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(false, "test-stream-name-6");
    }

    @Test
    public void veryLargeMessagesFailGracefullyWithBrokenElementConverter() {
        Throwable thrown =
                assertThrows(
                        JobExecutionException.class,
                        () ->
                                new Scenario()
                                        .withNumberOfElementsToSend(5)
                                        .withSizeOfMessageBytes(2500)
                                        .withBufferMaxSizeBytes(8192)
                                        .withExpectedElements(5)
                                        .withKinesaliteStreamName("test-stream-name-7")
                                        .withSinkConnectionStreamName("test-stream-name-7")
                                        .withElementConverter(partitionKeyTooLongElementConverter)
                                        .runScenario());
        assertEquals(
                "Encountered an exception while persisting records, not retrying due to {failOnError} being set.",
                thrown.getCause().getCause().getMessage());
    }

    private class Scenario {
        private int numberOfElementsToSend = 50;
        private int sizeOfMessageBytes = 25;
        private int bufferMaxTimeMS = 1000;
        private int bufferMaxSizeBytes = 819200;
        private int maxInflightReqs = 1;
        private int maxBatchSize = 50;
        private int expectedElements = 50;
        private boolean failOnError = false;
        private String kinesaliteStreamName;
        private String sinkConnectionStreamName;
        private ElementConverter<String, PutRecordsRequestEntry> elementConverter =
                KinesisDataStreamsSinkITCase.this.elementConverter;

        public void runScenario() throws Exception {
            prepareStream(kinesaliteStreamName);

            DataStream<String> stream =
                    env.addSource(
                                    new DataGeneratorSource<String>(
                                            RandomGenerator.stringGenerator(sizeOfMessageBytes),
                                            100,
                                            (long) numberOfElementsToSend))
                            .returns(String.class);

            Properties prop = new Properties();
            prop.setProperty(AWS_ENDPOINT, kinesalite.getHostEndpointUrl());
            prop.setProperty(AWS_ACCESS_KEY_ID, kinesalite.getAccessKey());
            prop.setProperty(AWS_SECRET_ACCESS_KEY, kinesalite.getSecretKey());
            prop.setProperty(AWS_REGION, kinesalite.getRegion().toString());
            prop.setProperty(TRUST_ALL_CERTIFICATES, "true");
            prop.setProperty(HTTP_PROTOCOL_VERSION, "HTTP1_1");

            KinesisDataStreamsSink<String> kdsSink =
                    KinesisDataStreamsSink.<String>builder()
                            .setElementConverter(elementConverter)
                            .setMaxTimeInBufferMS(bufferMaxTimeMS)
                            .setFlushOnBufferSizeInBytes(bufferMaxSizeBytes)
                            .setMaxInFlightRequests(maxInflightReqs)
                            .setMaxBatchSize(maxBatchSize)
                            .setFailOnError(failOnError)
                            .setMaxBufferedRequests(1000)
                            .setStreamName(sinkConnectionStreamName)
                            .setKinesisClientProperties(prop)
                            .setFailOnError(true)
                            .build();

            stream.sinkTo(kdsSink);

            env.execute("KDS Async Sink Example Program");

            String shardIterator =
                    kinesisClient
                            .getShardIterator(
                                    GetShardIteratorRequest.builder()
                                            .shardId(DEFAULT_FIRST_SHARD_NAME)
                                            .shardIteratorType(ShardIteratorType.TRIM_HORIZON)
                                            .streamName(kinesaliteStreamName)
                                            .build())
                            .get()
                            .shardIterator();

            assertEquals(
                    expectedElements,
                    kinesisClient
                            .getRecords(
                                    GetRecordsRequest.builder()
                                            .shardIterator(shardIterator)
                                            .build())
                            .get()
                            .records()
                            .size());
        }

        public Scenario withNumberOfElementsToSend(int numberOfElementsToSend) {
            this.numberOfElementsToSend = numberOfElementsToSend;
            return this;
        }

        public Scenario withSizeOfMessageBytes(int sizeOfMessageBytes) {
            this.sizeOfMessageBytes = sizeOfMessageBytes;
            return this;
        }

        public Scenario withBufferMaxTimeMS(int bufferMaxTimeMS) {
            this.bufferMaxTimeMS = bufferMaxTimeMS;
            return this;
        }

        public Scenario withBufferMaxSizeBytes(int bufferMaxSizeBytes) {
            this.bufferMaxSizeBytes = bufferMaxSizeBytes;
            return this;
        }

        public Scenario withMaxInflightReqs(int maxInflightReqs) {
            this.maxInflightReqs = maxInflightReqs;
            return this;
        }

        public Scenario withMaxBatchSize(int maxBatchSize) {
            this.maxBatchSize = maxBatchSize;
            return this;
        }

        public Scenario withExpectedElements(int expectedElements) {
            this.expectedElements = expectedElements;
            return this;
        }

        public Scenario withFailOnError(boolean failOnError) {
            this.failOnError = failOnError;
            return this;
        }

        public Scenario withSinkConnectionStreamName(String sinkConnectionStreamName) {
            this.sinkConnectionStreamName = sinkConnectionStreamName;
            return this;
        }

        public Scenario withKinesaliteStreamName(String kinesaliteStreamName) {
            this.kinesaliteStreamName = kinesaliteStreamName;
            return this;
        }

        public Scenario withElementConverter(
                ElementConverter<String, PutRecordsRequestEntry> elementConverter) {
            this.elementConverter = elementConverter;
            return this;
        }
    }

    private void testJobFatalFailureTerminatesCorrectlyWithFailOnErrorFlagSetTo(
            boolean failOnError, String streamName) {
        Throwable thrown =
                assertThrows(
                        JobExecutionException.class,
                        () ->
                                new Scenario()
                                        .withKinesaliteStreamName(streamName)
                                        .withSinkConnectionStreamName("non-existent-stream")
                                        .withFailOnError(failOnError)
                                        .runScenario());
        assertEquals(
                "Encountered non-recoverable exception", thrown.getCause().getCause().getMessage());
    }

    private void prepareStream(String testStreamName)
            throws InterruptedException, ExecutionException {
        kinesisClient
                .createStream(
                        CreateStreamRequest.builder()
                                .streamName(testStreamName)
                                .shardCount(1)
                                .build())
                .get();

        DescribeStreamResponse describeStream =
                kinesisClient
                        .describeStream(
                                DescribeStreamRequest.builder().streamName(testStreamName).build())
                        .get();

        while (describeStream.streamDescription().streamStatus() != StreamStatus.ACTIVE) {
            describeStream =
                    kinesisClient
                            .describeStream(
                                    DescribeStreamRequest.builder()
                                            .streamName(testStreamName)
                                            .build())
                            .get();
        }
    }
}
