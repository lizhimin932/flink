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

package org.apache.flink.table.runtime.operators.python.aggregate.arrow.batch;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.python.PythonFunctionRunner;
import org.apache.flink.python.PythonOptions;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.connector.Projection;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.operators.python.aggregate.arrow.AbstractArrowPythonAggregateFunctionOperator;
import org.apache.flink.table.runtime.utils.PassThroughPythonAggregateFunctionRunner;
import org.apache.flink.table.runtime.utils.PythonTestUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Test for {@link BatchArrowPythonGroupWindowAggregateFunctionOperator}. These test that:
 *
 * <ul>
 *   <li>FinishBundle is called when bundled element count reach to max bundle size
 *   <li>FinishBundle is called when bundled time reach to max bundle time
 * </ul>
 */
class BatchArrowPythonGroupWindowAggregateFunctionOperatorTest
        extends AbstractBatchArrowPythonAggregateFunctionOperatorTest {
    @Test
    void testGroupAggregateFunction() throws Exception {
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness =
                getTestHarness(new Configuration());
        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));

        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredByCount() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 6);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));

        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testFinishBundleTriggeredByTime() throws Exception {
        Configuration conf = new Configuration();
        conf.setInteger(PythonOptions.MAX_BUNDLE_SIZE, 10);
        conf.setLong(PythonOptions.MAX_BUNDLE_TIME_MILLS, 1000L);
        OneInputStreamOperatorTestHarness<RowData, RowData> testHarness = getTestHarness(conf);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c2", 0L, 0L), initialTime + 1));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c4", 1L, 6000L), initialTime + 2));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c1", "c6", 2L, 10000L), initialTime + 3));
        testHarness.processElement(
                new StreamRecord<>(newBinaryRow(true, "c2", "c8", 3L, 0L), initialTime + 3));
        assertOutputEquals(
                "FinishBundle should not be triggered.", expectedOutput, testHarness.getOutput());

        testHarness.setProcessingTime(1000L);
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                0L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                1L,
                                TimestampData.fromEpochMillis(5000L),
                                TimestampData.fromEpochMillis(15000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c1",
                                2L,
                                TimestampData.fromEpochMillis(10000L),
                                TimestampData.fromEpochMillis(20000L))));
        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();

        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(-5000L),
                                TimestampData.fromEpochMillis(5000L))));
        expectedOutput.add(
                new StreamRecord<>(
                        newRow(
                                true,
                                "c2",
                                3L,
                                TimestampData.fromEpochMillis(0L),
                                TimestampData.fromEpochMillis(10000L))));

        assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Override
    public LogicalType[] getOutputLogicalType() {
        return new LogicalType[] {
            DataTypes.STRING().getLogicalType(), DataTypes.BIGINT().getLogicalType()
        };
    }

    @Override
    public RowType getInputType() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("f1", new VarCharType()),
                        new RowType.RowField("f2", new VarCharType()),
                        new RowType.RowField("f3", new BigIntType()),
                        new RowType.RowField("rowTime", new BigIntType())));
    }

    @Override
    public RowType getOutputType() {
        return new RowType(
                Arrays.asList(
                        new RowType.RowField("f1", new VarCharType()),
                        new RowType.RowField("f2", new BigIntType()),
                        new RowType.RowField("windowStart", new TimestampType(3)),
                        new RowType.RowField("windowEnd", new TimestampType(3))));
    }

    @Override
    public AbstractArrowPythonAggregateFunctionOperator getTestOperator(
            Configuration config,
            PythonFunctionInfo[] pandasAggregateFunctions,
            RowType inputRowType,
            RowType outputRowType,
            int[] groupingSet,
            int[] udafInputOffsets) {

        RowType udfInputType = (RowType) Projection.of(udafInputOffsets).project(inputRowType);
        RowType udfOutputType =
                (RowType)
                        Projection.range(groupingSet.length, outputRowType.getFieldCount() - 2)
                                .project(outputRowType);

        // SlidingWindow(10000L, 5000L)
        return new PassThroughBatchArrowPythonGroupWindowAggregateFunctionOperator(
                config,
                pandasAggregateFunctions,
                inputRowType,
                udfInputType,
                udfOutputType,
                3,
                100000,
                10000L,
                5000L,
                new int[] {0, 1},
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "UdafInputProjection",
                        inputRowType,
                        udfInputType,
                        udafInputOffsets),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "GroupKey",
                        inputRowType,
                        (RowType) Projection.of(groupingSet).project(inputRowType),
                        groupingSet),
                ProjectionCodeGenerator.generateProjection(
                        new CodeGeneratorContext(
                                new Configuration(),
                                Thread.currentThread().getContextClassLoader()),
                        "GroupSet",
                        inputRowType,
                        (RowType) Projection.of(groupingSet).project(inputRowType),
                        groupingSet));
    }

    private static class PassThroughBatchArrowPythonGroupWindowAggregateFunctionOperator
            extends BatchArrowPythonGroupWindowAggregateFunctionOperator {

        PassThroughBatchArrowPythonGroupWindowAggregateFunctionOperator(
                Configuration config,
                PythonFunctionInfo[] pandasAggFunctions,
                RowType inputType,
                RowType udfInputType,
                RowType udfOutputType,
                int inputTimeFieldIndex,
                int maxLimitSize,
                long windowSize,
                long slideSize,
                int[] namedProperties,
                GeneratedProjection inputGeneratedProjection,
                GeneratedProjection groupKeyGeneratedProjection,
                GeneratedProjection groupSetGeneratedProjection) {
            super(
                    config,
                    pandasAggFunctions,
                    inputType,
                    udfInputType,
                    udfOutputType,
                    inputTimeFieldIndex,
                    maxLimitSize,
                    windowSize,
                    slideSize,
                    namedProperties,
                    inputGeneratedProjection,
                    groupKeyGeneratedProjection,
                    groupSetGeneratedProjection);
        }

        @Override
        public PythonFunctionRunner createPythonFunctionRunner() {
            return new PassThroughPythonAggregateFunctionRunner(
                    getRuntimeContext().getTaskName(),
                    PythonTestUtils.createTestProcessEnvironmentManager(),
                    udfInputType,
                    udfOutputType,
                    getFunctionUrn(),
                    getUserDefinedFunctionsProto(),
                    PythonTestUtils.createMockFlinkMetricContainer(),
                    false);
        }
    }
}
