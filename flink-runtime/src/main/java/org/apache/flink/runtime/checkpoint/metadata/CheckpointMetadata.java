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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.util.Disposable;

import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The metadata of a snapshot (checkpoint or savepoint). */
public class CheckpointMetadata implements Disposable {

    /** The checkpoint ID. */
    private final long checkpointId;

    /** The operator states. */
    private final Collection<OperatorState> operatorStates;

    /** The states generated by the CheckpointCoordinator. */
    private final Collection<MasterState> masterStates;

    public CheckpointMetadata(
            long checkpointId,
            Collection<OperatorState> operatorStates,
            Collection<MasterState> masterStates) {
        this.checkpointId = checkpointId;
        this.operatorStates = operatorStates;
        this.masterStates = checkNotNull(masterStates, "masterStates");
    }

    public long getCheckpointId() {
        return checkpointId;
    }

    public Collection<OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public Collection<MasterState> getMasterStates() {
        return masterStates;
    }

    @Override
    public void dispose() throws Exception {
        for (OperatorState operatorState : operatorStates) {
            operatorState.discardState();
        }
        operatorStates.clear();
        masterStates.clear();
    }

    @Override
    public String toString() {
        return "Checkpoint Metadata";
    }
}
