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

package org.apache.flink.state.api.input.splits;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.state.OperatorStateHandle;

import javax.annotation.Nonnull;

import java.util.Collections;

/** An input split containing state handles for operator state. */
@Internal
public final class OperatorStateInputSplit implements PrioritizedOperatorSubtaskStateInputSplit {

    private static final long serialVersionUID = -1892383531558135420L;

    private final StateObjectCollection<OperatorStateHandle> managedOperatorState;

    private final int splitNum;

    public OperatorStateInputSplit(
            StateObjectCollection<OperatorStateHandle> managedOperatorState, int splitNum) {
        this.managedOperatorState = managedOperatorState;
        this.splitNum = splitNum;
    }

    @Override
    public int getSplitNumber() {
        return splitNum;
    }

    @Nonnull
    public StateObjectCollection<OperatorStateHandle> getPrioritizedManagedOperatorState() {
        return this.managedOperatorState;
    }

    @Override
    public PrioritizedOperatorSubtaskState getPrioritizedOperatorSubtaskState() {
        final OperatorSubtaskState subtaskState =
                OperatorSubtaskState.builder()
                        .setManagedOperatorState(managedOperatorState)
                        .build();
        return new PrioritizedOperatorSubtaskState.Builder(subtaskState, Collections.emptyList())
                .build();
    }
}
