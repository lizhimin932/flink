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

package org.apache.flink.streaming.connectors.dynamodb.batch;

import org.apache.flink.streaming.connectors.dynamodb.DynamoDbProducer;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteRequest;
import org.apache.flink.streaming.connectors.dynamodb.ProducerWriteResponse;
import org.apache.flink.streaming.connectors.dynamodb.retry.WriterRetryPolicy;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.concurrent.Callable;

/** Provides a callable to write ProducerWriteRequest. */
public class BatchWriterProvider {

    private final DynamoDbClient client;
    private final WriterRetryPolicy retryPolicy;
    private final DynamoDbProducer.Listener listener;

    public BatchWriterProvider(
            DynamoDbClient client,
            WriterRetryPolicy retryPolicy,
            DynamoDbProducer.Listener listener) {
        this.client = client;
        this.retryPolicy = retryPolicy;
        this.listener = listener;
    }

    public Callable<ProducerWriteResponse> createWriter(ProducerWriteRequest request) {
        return new DynamoDbBatchWriter(client, retryPolicy, listener, request);
    }
}
