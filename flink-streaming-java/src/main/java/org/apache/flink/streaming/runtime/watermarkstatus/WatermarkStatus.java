/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

/**
 * A Watermark Status element informs stream tasks whether or not they should continue to expect
 * watermarks from the input stream that sent them. There are 2 kinds of status, namely {@link
 * WatermarkStatus#IDLE} and {@link WatermarkStatus#ACTIVE}. Watermark Status elements are generated
 * at the sources, and may be propagated through the tasks of the topology. They directly infer the
 * current status of the emitting task; a {@link SourceStreamTask} or {@link StreamTask} emits a
 * {@link WatermarkStatus#IDLE} if it will temporarily halt to emit any watermarks (i.e. is idle),
 * and emits a {@link WatermarkStatus#ACTIVE} once it resumes to do so (i.e. is active). Tasks are
 * responsible for propagating their status further downstream once they toggle between being idle
 * and active. The cases that source tasks and downstream tasks are considered either idle or active
 * is explained below:
 *
 * <ul>
 *   <li>Source tasks: A source task is considered to be idle if its head operator, i.e. a {@link
 *       StreamSource}, will not emit watermarks for an indefinite amount of time. This is the case,
 *       for example, for Flink's Kafka Consumer, where sources might initially have no assigned
 *       partitions to read from, or no records can be read from the assigned partitions. Once the
 *       head {@link StreamSource} operator detects that it will resume emitting data, the source
 *       task is considered to be active. {@link StreamSource}s are responsible for toggling the
 *       status of the containing source task and ensuring that no watermarks will be emitted while
 *       the task is idle. This guarantee should be enforced on sources through {@link
 *       org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext}
 *       implementations.
 *   <li>Downstream tasks: a downstream task is considered to be idle if all its input streams are
 *       idle, i.e. the last received Watermark Status element from all input streams is a {@link
 *       WatermarkStatus#IDLE}. As long as one of its input streams is active, i.e. the last
 *       received Watermark Status element from the input stream is {@link WatermarkStatus#ACTIVE},
 *       the task is active.
 * </ul>
 *
 * <p>Watermark Status elements received at downstream tasks also affect and control how their
 * operators process and advance their watermarks. The below describes the effects (the logic is
 * implemented as a {@link StatusWatermarkValve} which downstream tasks should use for such
 * purposes):
 *
 * <ul>
 *   <li>Since there may be watermark generators that might produce watermarks anywhere in the
 *       middle of topologies regardless of whether there are input data at the operator, the
 *       current status of the task must be checked before forwarding watermarks emitted from an
 *       operator. If the status is actually idle, the watermark must be blocked.
 *   <li>For downstream tasks with multiple input streams, the watermarks of input streams that are
 *       temporarily idle, or has resumed to be active but its watermark is behind the overall min
 *       watermark of the operator, should not be accounted for when deciding whether or not to
 *       advance the watermark and propagated through the operator chain.
 * </ul>
 *
 * <p>Note that to notify downstream tasks that a source task is permanently closed and will no
 * longer send any more elements, the source should still send a {@link Watermark#MAX_WATERMARK}
 * instead of {@link WatermarkStatus#IDLE}. Watermark Status elements only serve as markers for
 * temporary status.
 */
@Internal
public final class WatermarkStatus extends StreamElement {

    public static final int IDLE_STATUS = -1;
    public static final int ACTIVE_STATUS = 0;

    public static final WatermarkStatus IDLE = new WatermarkStatus(IDLE_STATUS);
    public static final WatermarkStatus ACTIVE = new WatermarkStatus(ACTIVE_STATUS);

    public final int status;

    public WatermarkStatus(int status) {
        if (status != IDLE_STATUS && status != ACTIVE_STATUS) {
            throw new IllegalArgumentException(
                    "Invalid status value for WatermarkStatus; "
                            + "allowed values are "
                            + ACTIVE_STATUS
                            + " (for ACTIVE) and "
                            + IDLE_STATUS
                            + " (for IDLE).");
        }

        this.status = status;
    }

    public boolean isIdle() {
        return this.status == IDLE_STATUS;
    }

    public boolean isActive() {
        return !isIdle();
    }

    public int getStatus() {
        return status;
    }

    @Override
    public boolean equals(Object o) {
        return this == o
                || o != null
                        && o.getClass() == WatermarkStatus.class
                        && ((WatermarkStatus) o).status == this.status;
    }

    @Override
    public int hashCode() {
        return status;
    }

    @Override
    public String toString() {
        String statusStr = (status == ACTIVE_STATUS) ? "ACTIVE" : "IDLE";
        return "WatermarkStatus(" + statusStr + ")";
    }
}
