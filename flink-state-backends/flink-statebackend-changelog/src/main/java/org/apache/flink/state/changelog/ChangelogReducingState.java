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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.State;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.internal.InternalReducingState;
import org.apache.flink.state.changelog.restore.ChangelogApplierFactory;
import org.apache.flink.state.changelog.restore.StateChangeApplier;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Collection;

/**
 * Delegated partitioned {@link ReducingState} that forwards changes to {@link StateChange} upon
 * {@link ReducingState} is updated.
 *
 * @param <K> The type of the key.
 * @param <N> The type of the namespace.
 * @param <V> The type of the value.
 */
class ChangelogReducingState<K, N, V>
        extends AbstractChangelogState<K, N, V, InternalReducingState<K, N, V>>
        implements InternalReducingState<K, N, V> {

    private final InternalKeyContext<K> keyContext;

    ChangelogReducingState(
            InternalReducingState<K, N, V> delegatedState,
            KvStateChangeLogger<V, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        super(delegatedState, changeLogger);
        this.keyContext = keyContext;
    }

    @Override
    public void mergeNamespaces(N target, Collection<N> sources) throws Exception {
        delegatedState.mergeNamespaces(target, sources);
        // do not simply store the new value for the target namespace
        // because old namespaces need to be removed
        changeLogger.namespacesMerged(target, sources);
    }

    @Override
    public V getInternal() throws Exception {
        return delegatedState.getInternal();
    }

    @Override
    public void updateInternal(V valueToStore) throws Exception {
        delegatedState.updateInternal(valueToStore);
        changeLogger.valueUpdatedInternal(valueToStore, getCurrentNamespace());
    }

    @Override
    public V get() throws Exception {
        return delegatedState.get();
    }

    @Override
    public void add(V value) throws Exception {
        delegatedState.add(value);
        // On recovery, we must end up with the same state as we have now in delegatedState.
        // However, the actual state update is generated by a user-defined function
        // which is not necessarily deterministic and immediately known on recovery.
        // Therefore, we have to store the whole current state (and there is no need to store the
        // added value).
        // Additionally, the updated state can be smaller than the added value (e.g. a counter);
        // or bigger (e.g. a list). But this can't be determined easily.
        changeLogger.valueUpdatedInternal(delegatedState.getInternal(), getCurrentNamespace());
    }

    @Override
    public void clear() {
        delegatedState.clear();
        try {
            changeLogger.valueCleared(getCurrentNamespace());
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    @SuppressWarnings("unchecked")
    static <K, N, SV, S extends State, IS extends S> IS create(
            InternalKvState<K, N, SV> reducingState,
            KvStateChangeLogger<SV, N> changeLogger,
            InternalKeyContext<K> keyContext) {
        return (IS)
                new ChangelogReducingState<>(
                        (InternalReducingState<K, N, SV>) reducingState, changeLogger, keyContext);
    }

    @Override
    public StateChangeApplier getChangeApplier(ChangelogApplierFactory factory) {
        return factory.forReducing(delegatedState, keyContext);
    }
}
