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

package org.apache.flink.runtime.io.network.api;

import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.event.RuntimeEvent;

import java.io.IOException;
import java.util.Objects;

/**
 * This event indicates there will be no more data records in a subpartition. There still might be
 * other events, in particular {@link CheckpointBarrier CheckpointBarriers} traveling. The {@link
 * EndOfData} is acknowledged by the downstream task. That way we can safely assume the downstream
 * task has consumed all the produced records and therefore we can perform a final checkpoint for
 * the upstream task.
 *
 * @see <a href="https://cwiki.apache.org/confluence/x/mw-ZCQ">FLIP-147</a>
 */
public class EndOfData extends RuntimeEvent {

    private final boolean shouldDrain;

    // ------------------------------------------------------------------------

    public EndOfData(boolean shouldDrain) {
        this.shouldDrain = shouldDrain;
    }

    public boolean shouldDrain() {
        return shouldDrain;
    }

    // ------------------------------------------------------------------------

    //
    //  These methods are inherited form the generic serialization of AbstractEvent
    //  but would require the CheckpointBarrier to be mutable. Since all serialization
    //  for events goes through the EventSerializer class, which has special serialization
    //  for the CheckpointBarrier, we don't need these methods
    //
    @Override
    public void write(DataOutputView out) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    @Override
    public void read(DataInputView in) throws IOException {
        throw new UnsupportedOperationException("This method should never be called");
    }

    // ------------------------------------------------------------------------

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        EndOfData endOfData = (EndOfData) o;
        return shouldDrain == endOfData.shouldDrain;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shouldDrain);
    }

    @Override
    public String toString() {
        return "EndOfData{shouldDrain=" + shouldDrain + '}';
    }
}
