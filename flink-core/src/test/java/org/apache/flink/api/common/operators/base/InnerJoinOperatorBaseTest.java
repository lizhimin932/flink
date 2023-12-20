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

package org.apache.flink.api.common.operators.base;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.TaskInfoImpl;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatJoinFunction;
import org.apache.flink.api.common.functions.util.RuntimeUDFContext;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;

/** The test for inner join operator. */
public class InnerJoinOperatorBaseTest implements Serializable {

    @Test
    void testJoinPlain() {
        final FlatJoinFunction<String, String, Integer> joiner =
                (first, second, out) -> {
                    out.collect(first.length());
                    out.collect(second.length());
                };

        @SuppressWarnings({"rawtypes", "unchecked"})
        InnerJoinOperatorBase<String, String, Integer, FlatJoinFunction<String, String, Integer>>
                base =
                        new InnerJoinOperatorBase(
                                joiner,
                                new BinaryOperatorInformation(
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO),
                                new int[0],
                                new int[0],
                                "TestJoiner");

        List<String> inputData1 = new ArrayList<>(Arrays.asList("foo", "bar", "foobar"));
        List<String> inputData2 = new ArrayList<>(Arrays.asList("foobar", "foo"));
        List<Integer> expected = new ArrayList<>(Arrays.asList(3, 3, 6, 6));

        try {
            ExecutionConfig executionConfig = new ExecutionConfig();
            executionConfig.disableObjectReuse();
            List<Integer> resultSafe =
                    base.executeOnCollections(inputData1, inputData2, null, executionConfig);
            executionConfig.enableObjectReuse();
            List<Integer> resultRegular =
                    base.executeOnCollections(inputData1, inputData2, null, executionConfig);

            assertThat(resultSafe).isEqualTo(expected);
            assertThat(resultRegular).isEqualTo(expected);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testJoinRich() {
        final AtomicBoolean opened = new AtomicBoolean(false);
        final AtomicBoolean closed = new AtomicBoolean(false);
        final String taskName = "Test rich join function";

        final RichFlatJoinFunction<String, String, Integer> joiner =
                new RichFlatJoinFunction<String, String, Integer>() {
                    @Override
                    public void open(OpenContext openContext) {
                        opened.compareAndSet(false, true);
                        assertThat(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()).isZero();
                        assertThat(getRuntimeContext().getTaskInfo().getNumberOfParallelSubtasks()).isEqualTo(1);
                    }

                    @Override
                    public void close() {
                        closed.compareAndSet(false, true);
                    }

                    @Override
                    public void join(String first, String second, Collector<Integer> out) {
                        out.collect(first.length());
                        out.collect(second.length());
                    }
                };

        InnerJoinOperatorBase<
                        String, String, Integer, RichFlatJoinFunction<String, String, Integer>>
                base =
                        new InnerJoinOperatorBase<>(
                                joiner,
                                new BinaryOperatorInformation<>(
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.STRING_TYPE_INFO,
                                        BasicTypeInfo.INT_TYPE_INFO),
                                new int[0],
                                new int[0],
                                taskName);

        final List<String> inputData1 = new ArrayList<>(Arrays.asList("foo", "bar", "foobar"));
        final List<String> inputData2 = new ArrayList<>(Arrays.asList("foobar", "foo"));
        final List<Integer> expected = new ArrayList<>(Arrays.asList(3, 3, 6, 6));

        try {
            final TaskInfo taskInfo = new TaskInfoImpl(taskName, 1, 0, 1, 0);
            final HashMap<String, Accumulator<?, ?>> accumulatorMap = new HashMap<>();
            final HashMap<String, Future<Path>> cpTasks = new HashMap<>();

            ExecutionConfig executionConfig = new ExecutionConfig();

            executionConfig.disableObjectReuse();
            List<Integer> resultSafe =
                    base.executeOnCollections(
                            inputData1,
                            inputData2,
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            executionConfig.enableObjectReuse();
            List<Integer> resultRegular =
                    base.executeOnCollections(
                            inputData1,
                            inputData2,
                            new RuntimeUDFContext(
                                    taskInfo,
                                    null,
                                    executionConfig,
                                    cpTasks,
                                    accumulatorMap,
                                    UnregisteredMetricsGroup.createOperatorMetricGroup()),
                            executionConfig);

            assertThat(resultSafe).isEqualTo(expected);
            assertThat(resultRegular).isEqualTo(expected);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }

        assertThat(opened.get()).isTrue();
        assertThat(closed.get()).isTrue();
    }
}
