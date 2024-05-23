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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.RichCoMapFunction;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link org.apache.flink.streaming.api.operators.co.CoStreamMap}. These test that:
 *
 * <ul>
 *   <li>RichFunction methods are called correctly
 *   <li>Timestamps of processed elements match the input timestamp
 *   <li>Watermarks are correctly forwarded
 * </ul>
 */
class CoStreamMapTest implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final class MyCoMap implements CoMapFunction<Double, Integer, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String map1(Double value) {
            return value.toString();
        }

        @Override
        public String map2(Integer value) {
            return value.toString();
        }
    }

    @Test
    void testCoMap() throws Exception {
        CoStreamMap<Double, Integer, String> operator =
                new CoStreamMap<Double, Integer, String>(new MyCoMap());

        TwoInputStreamOperatorTestHarness<Double, Integer, String> testHarness =
                new TwoInputStreamOperatorTestHarness<Double, Integer, String>(operator);

        long initialTime = 0L;
        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<Object>();

        testHarness.open();

        testHarness.processElement1(new StreamRecord<Double>(1.1d, initialTime + 1));
        testHarness.processElement1(new StreamRecord<Double>(1.2d, initialTime + 2));
        testHarness.processElement1(new StreamRecord<Double>(1.3d, initialTime + 3));
        testHarness.processWatermark1(new WatermarkEvent(new TimestampWatermark(initialTime + 3)));
        testHarness.processElement1(new StreamRecord<Double>(1.4d, initialTime + 4));
        testHarness.processElement1(new StreamRecord<Double>(1.5d, initialTime + 5));

        testHarness.processElement2(new StreamRecord<Integer>(1, initialTime + 1));
        testHarness.processElement2(new StreamRecord<Integer>(2, initialTime + 2));
        testHarness.processWatermark2(new WatermarkEvent(new TimestampWatermark(initialTime + 2)));

        testHarness.processElement2(new StreamRecord<Integer>(3, initialTime + 3));
        testHarness.processElement2(new StreamRecord<Integer>(4, initialTime + 4));
        testHarness.processElement2(new StreamRecord<Integer>(5, initialTime + 5));

        expectedOutput.add(new StreamRecord<String>("1.1", initialTime + 1));
        expectedOutput.add(new StreamRecord<String>("1.2", initialTime + 2));
        expectedOutput.add(new StreamRecord<String>("1.3", initialTime + 3));
        expectedOutput.add(new StreamRecord<String>("1.4", initialTime + 4));
        expectedOutput.add(new StreamRecord<String>("1.5", initialTime + 5));

        expectedOutput.add(new StreamRecord<String>("1", initialTime + 1));
        expectedOutput.add(new StreamRecord<String>("2", initialTime + 2));
        expectedOutput.add(new WatermarkEvent(new TimestampWatermark(initialTime + 2)));
        expectedOutput.add(new StreamRecord<String>("3", initialTime + 3));
        expectedOutput.add(new StreamRecord<String>("4", initialTime + 4));
        expectedOutput.add(new StreamRecord<String>("5", initialTime + 5));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());
    }

    @Test
    void testOpenClose() throws Exception {
        CoStreamMap<Double, Integer, String> operator =
                new CoStreamMap<Double, Integer, String>(new TestOpenCloseCoMapFunction());

        TwoInputStreamOperatorTestHarness<Double, Integer, String> testHarness =
                new TwoInputStreamOperatorTestHarness<Double, Integer, String>(operator);

        long initialTime = 0L;

        testHarness.open();

        testHarness.processElement1(new StreamRecord<Double>(74d, initialTime));
        testHarness.processElement2(new StreamRecord<Integer>(42, initialTime));

        testHarness.close();

        assertThat(TestOpenCloseCoMapFunction.closeCalled)
                .as("RichFunction methods where not called.")
                .isTrue();
        assertThat(testHarness.getOutput()).isNotEmpty();
    }

    // This must only be used in one test, otherwise the static fields will be changed
    // by several tests concurrently
    private static class TestOpenCloseCoMapFunction
            extends RichCoMapFunction<Double, Integer, String> {
        private static final long serialVersionUID = 1L;

        public static boolean openCalled = false;
        public static boolean closeCalled = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            assertThat(closeCalled).as("Close was called before open.").isFalse();
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            assertThat(openCalled).as("Open was not called before close.").isTrue();
            closeCalled = true;
        }

        @Override
        public String map1(Double value) throws Exception {
            assertThat(openCalled).as("Open was not called before run.").isTrue();
            return value.toString();
        }

        @Override
        public String map2(Integer value) throws Exception {
            assertThat(openCalled).as("Open was not called before run.").isTrue();
            return value.toString();
        }
    }
}
