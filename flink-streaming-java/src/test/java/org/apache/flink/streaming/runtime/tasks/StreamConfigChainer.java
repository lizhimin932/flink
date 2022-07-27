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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class to build StreamConfig for chain of operators. */
public class StreamConfigChainer<OWNER> {
    public static final int MAIN_NODE_ID = 0;
    private final OWNER owner;
    private final StreamConfig headConfig;
    private final Map<Integer, StreamConfig> chainedConfigs = new HashMap<>();
    private final int numberOfNonChainedOutputs;
    private int bufferTimeout;

    private StreamConfig tailConfig;
    private int chainIndex = MAIN_NODE_ID;

    StreamConfigChainer(
            OperatorID headOperatorID,
            StreamConfig headConfig,
            OWNER owner,
            int numberOfNonChainedOutputs) {
        this.owner = checkNotNull(owner);
        this.headConfig = checkNotNull(headConfig);
        this.tailConfig = checkNotNull(headConfig);
        this.numberOfNonChainedOutputs = numberOfNonChainedOutputs;

        head(headOperatorID);
    }

    private void head(OperatorID headOperatorID) {
        headConfig.setOperatorID(headOperatorID);
        headConfig.setChainStart();
        headConfig.setChainIndex(chainIndex);
    }

    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<T, T> operator,
            TypeSerializer<T> typeSerializer,
            boolean createKeyedStateBackend) {
        return chain(operatorID, operator, typeSerializer, typeSerializer, createKeyedStateBackend);
    }

    public <T> StreamConfigChainer<OWNER> chain(
            OneInputStreamOperator<T, T> operator, TypeSerializer<T> typeSerializer) {
        return chain(new OperatorID(), operator, typeSerializer);
    }

    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<T, T> operator,
            TypeSerializer<T> typeSerializer) {
        return chain(operatorID, operator, typeSerializer, typeSerializer, false);
    }

    public <T> StreamConfigChainer<OWNER> chain(
            OneInputStreamOperatorFactory<T, T> operatorFactory, TypeSerializer<T> typeSerializer) {
        return chain(new OperatorID(), operatorFactory, typeSerializer);
    }

    public <T> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperatorFactory<T, T> operatorFactory,
            TypeSerializer<T> typeSerializer) {
        return chain(operatorID, operatorFactory, typeSerializer, typeSerializer, false);
    }

    private <IN, OUT> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            OneInputStreamOperator<IN, OUT> operator,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<OUT> outputSerializer,
            boolean createKeyedStateBackend) {
        return chain(
                operatorID,
                SimpleOperatorFactory.of(operator),
                inputSerializer,
                outputSerializer,
                createKeyedStateBackend);
    }

    public <IN, OUT> StreamConfigChainer<OWNER> chain(
            OperatorID operatorID,
            StreamOperatorFactory<OUT> operatorFactory,
            TypeSerializer<IN> inputSerializer,
            TypeSerializer<OUT> outputSerializer,
            boolean createKeyedStateBackend) {

        chainIndex++;

        StreamEdge streamEdge =
                new StreamEdge(
                        new StreamNode(
                                tailConfig.getChainIndex(),
                                null,
                                null,
                                (StreamOperator<?>) null,
                                null,
                                null),
                        new StreamNode(
                                chainIndex, null, null, (StreamOperator<?>) null, null, null),
                        0,
                        null,
                        null);
        streamEdge.setBufferTimeout(bufferTimeout);
        tailConfig.setChainedOutputs(Collections.singletonList(streamEdge));
        tailConfig = new StreamConfig(new Configuration());
        tailConfig.setStreamOperatorFactory(checkNotNull(operatorFactory));
        tailConfig.setOperatorID(checkNotNull(operatorID));
        tailConfig.setupNetworkInputs(inputSerializer);
        tailConfig.setTypeSerializerOut(outputSerializer);
        if (createKeyedStateBackend) {
            // used to test multiple stateful operators chained in a single task.
            tailConfig.setStateKeySerializer(inputSerializer);
            tailConfig.setStateBackendUsesManagedMemory(true);
            tailConfig.setManagedMemoryFractionOperatorOfUseCase(
                    ManagedMemoryUseCase.STATE_BACKEND, 1.0);
        }
        tailConfig.setChainIndex(chainIndex);
        tailConfig.serializeAllConfigs();

        chainedConfigs.put(chainIndex, tailConfig);

        return this;
    }

    public OWNER finish() {
        checkState(chainIndex > 0, "Use finishForSingletonOperatorChain");
        List<StreamEdge> outEdgesInOrder = new LinkedList<StreamEdge>();

        StreamNode sourceVertex =
                new StreamNode(chainIndex, null, null, (StreamOperator<?>) null, null, null);
        for (int i = 0; i < numberOfNonChainedOutputs; ++i) {
            StreamEdge streamEdge =
                    new StreamEdge(
                            sourceVertex,
                            new StreamNode(
                                    chainIndex + i,
                                    null,
                                    null,
                                    (StreamOperator<?>) null,
                                    null,
                                    null),
                            0,
                            new BroadcastPartitioner<>(),
                            null);
            streamEdge.setBufferTimeout(1);
            outEdgesInOrder.add(streamEdge);
        }

        tailConfig.setChainEnd();
        tailConfig.setNumberOfOutputs(numberOfNonChainedOutputs);
        tailConfig.setOutEdgesInOrder(outEdgesInOrder);
        tailConfig.setNonChainedOutputs(outEdgesInOrder);
        chainedConfigs.values().forEach(StreamConfig::serializeAllConfigs);

        headConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfigs);
        headConfig.setOutEdgesInOrder(outEdgesInOrder);
        headConfig.serializeAllConfigs();

        return owner;
    }

    public <OUT> OWNER finishForSingletonOperatorChain(TypeSerializer<OUT> outputSerializer) {

        checkState(chainIndex == 0, "Use finishForSingletonOperatorChain");
        checkState(headConfig == tailConfig);
        StreamOperator<OUT> dummyOperator =
                new AbstractStreamOperator<OUT>() {
                    private static final long serialVersionUID = 1L;
                };
        List<StreamEdge> outEdgesInOrder = new LinkedList<>();

        StreamNode sourceVertexDummy =
                new StreamNode(
                        MAIN_NODE_ID,
                        "group",
                        null,
                        dummyOperator,
                        "source dummy",
                        SourceStreamTask.class);
        for (int i = 0; i < numberOfNonChainedOutputs; ++i) {
            StreamNode targetVertexDummy =
                    new StreamNode(
                            MAIN_NODE_ID + 1 + i,
                            "group",
                            null,
                            dummyOperator,
                            "target dummy",
                            SourceStreamTask.class);

            outEdgesInOrder.add(
                    new StreamEdge(
                            sourceVertexDummy,
                            targetVertexDummy,
                            0,
                            new BroadcastPartitioner<>(),
                            null));
        }

        headConfig.setVertexID(0);
        headConfig.setNumberOfOutputs(1);
        headConfig.setOutEdgesInOrder(outEdgesInOrder);
        headConfig.setNonChainedOutputs(outEdgesInOrder);
        chainedConfigs.values().forEach(StreamConfig::serializeAllConfigs);
        headConfig.setAndSerializeTransitiveChainedTaskConfigs(chainedConfigs);
        headConfig.setOutEdgesInOrder(outEdgesInOrder);
        headConfig.setTypeSerializerOut(outputSerializer);
        headConfig.serializeAllConfigs();

        return owner;
    }

    public StreamConfigChainer<OWNER> name(String name) {
        tailConfig.setOperatorName(name);
        return this;
    }

    public void setBufferTimeout(int bufferTimeout) {
        this.bufferTimeout = bufferTimeout;
    }
}
