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

package org.apache.flink.streaming.util.watermark;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.WatermarkDeclaration;
import org.apache.flink.api.common.eventtime.TimestampWatermark;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.datastream.api.WatermarkDeclarable;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.watermark.WatermarkEvent;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

@Public
public class WatermarkUtils {

    public static final WatermarkEvent MAX_TIMESTAMP_WATEMMARK_EVENT =
            new WatermarkEvent(TimestampWatermark.MAX_WATERMARK);

    public static final WatermarkEvent UNINITIALIZED_TIMESTAMP_WATEMMARK_EVENT =
            new WatermarkEvent(TimestampWatermark.UNINITIALIZED);

    public static WatermarkEvent createWatermarkEventFromTimestamp(long timestamp) {
        return new WatermarkEvent(new TimestampWatermark(timestamp));
    }

    public static Optional<Long> getTimestamp(WatermarkEvent event) {
        Watermark watermark = event.getWatermark();
        if (watermark instanceof TimestampWatermark) {
            return Optional.of(((TimestampWatermark) watermark).getTimestamp());
        } else {
            return Optional.empty();
        }
    }

    public static Set<Class<? extends WatermarkDeclaration>> getWatermarkDeclarations(
            StreamOperator<?> streamOperator) {
        return (streamOperator instanceof WatermarkDeclarable)
                ? ((WatermarkDeclarable) streamOperator).watermarkDeclarations()
                : Collections.emptySet();
    }
}
