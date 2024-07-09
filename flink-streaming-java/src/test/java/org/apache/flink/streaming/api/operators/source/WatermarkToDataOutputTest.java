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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.watermark.WatermarkUtils;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for the {@link WatermarkToDataOutput}. */
class WatermarkToDataOutputTest {

    @Test
    void testInitialZeroWatermark() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(0L));

        assertThat(testingOutput.events)
                .contains(WatermarkUtils.createWatermarkEventFromTimestamp(0L));
    }

    @Test
    void testWatermarksDoNotRegress() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(12L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(17L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(10L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(18L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(17L));
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(18L));

        assertThat(testingOutput.events)
                .contains(
                        WatermarkUtils.createWatermarkEventFromTimestamp(12L),
                        WatermarkUtils.createWatermarkEventFromTimestamp(17L),
                        WatermarkUtils.createWatermarkEventFromTimestamp(18L));
    }

    @Test
    void becomingActiveEmitsStatus() {
        final CollectingDataOutput<Object> testingOutput = new CollectingDataOutput<>();
        final WatermarkToDataOutput wmOutput = new WatermarkToDataOutput(testingOutput);

        wmOutput.markIdle();
        wmOutput.emitWatermark(new org.apache.flink.api.common.eventtime.TimestampWatermark(100L));

        assertThat(testingOutput.events)
                .contains(
                        WatermarkStatus.IDLE,
                        WatermarkStatus.ACTIVE,
                        WatermarkUtils.createWatermarkEventFromTimestamp(100L));
    }
}
