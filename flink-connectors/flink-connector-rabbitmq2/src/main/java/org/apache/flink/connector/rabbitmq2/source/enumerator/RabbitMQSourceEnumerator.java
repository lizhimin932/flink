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

package org.apache.flink.connector.rabbitmq2.source.enumerator;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSourceSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * The source enumerator provides the source readers with the split. All source readers receive the
 * same split as it only contains information about the connection and in case of exactly-once, the
 * seen correlation ids. But in this case, the enumerator makes sure that at maximum one source
 * reader receives the split. During exactly-once if multiple reader should be assigned a split a
 * {@link RuntimeException} is thrown.
 */
public class RabbitMQSourceEnumerator
        implements SplitEnumerator<RabbitMQSourceSplit, RabbitMQSourceEnumState> {
    private final SplitEnumeratorContext<RabbitMQSourceSplit> context;
    private final ConsistencyMode consistencyMode;
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceEnumerator.class);
    private RabbitMQSourceSplit split;

    public RabbitMQSourceEnumerator(
            SplitEnumeratorContext<RabbitMQSourceSplit> context,
            ConsistencyMode consistencyMode,
            RabbitMQConnectionConfig connectionConfig,
            String rmqQueueName,
            RabbitMQSourceEnumState enumState) {
        // The enumState is not used since the enumerator has no state in this architecture.
        this(context, consistencyMode, connectionConfig, rmqQueueName);
    }

    public RabbitMQSourceEnumerator(
            SplitEnumeratorContext<RabbitMQSourceSplit> context,
            ConsistencyMode consistencyMode,
            RabbitMQConnectionConfig connectionConfig,
            String rmqQueueName) {

        if (consistencyMode == ConsistencyMode.EXACTLY_ONCE && context.currentParallelism() > 1) {
            throw new RuntimeException(
                    "The consistency mode is exactly-once and a parallelism higher than one was defined. "
                            + "For exactly once a parallelism higher than one is forbidden.");
        }

        this.context = context;
        this.consistencyMode = consistencyMode;
        this.split = new RabbitMQSourceSplit(connectionConfig, rmqQueueName);
    }

    @Override
    public void start() {
        LOG.info("Start RabbitMQ source enumerator");
        System.out.println(context.currentParallelism());
    }

    @Override
    public void handleSplitRequest(int i, @Nullable String s) {
        LOG.info("Split request from reader " + i);
        assignSplitToReader(i, split);
    }

    @Override
    public void addSplitsBack(List<RabbitMQSourceSplit> list, int i) {
        if (list.size() == 0) {
            return;
        }

        // Every Source Reader will only receive one split, thus we will never get back more.
        if (list.size() != 1) {
            throw new RuntimeException("There should only be one split added back at a time. per reader");
        }

        LOG.info("Split returned from reader " + i);
        // In case of exactly-once (parallelism 1) the single split gets updated with the
        // correlation ids and in case of a recovery we have to store this split until we can
        // assign it to the recovered reader.
        split = list.get(0);
    }

    /**
     * In the case of exactly-once multiple readers are not allowed.
     *
     * @see RabbitMQSourceEnumerator#assignSplitToReader(int, RabbitMQSourceSplit)
     * @param i reader id
     */
    @Override
    public void addReader(int i) {}

    /** @return empty enum state object */
    @Override
    public RabbitMQSourceEnumState snapshotState() {
        return new RabbitMQSourceEnumState();
    }

    @Override
    public void close() {}

    private void assignSplitToReader(int readerId, RabbitMQSourceSplit split) {
        SplitsAssignment<RabbitMQSourceSplit> assignment = new SplitsAssignment<>(split, readerId);
        context.assignSplits(assignment);
    }
}
