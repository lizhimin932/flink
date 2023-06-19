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

package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Provider that extends for {@link DataStreamScanProvider} to let DataStreamScanProvider support
 * expose inner source.
 *
 * <p>Usually a {@link SourceProvider} expose {@link Source}, {@link SourceFunctionProvider} expose
 * {@link SourceFunction} and {@link InputFormatProvider} expose {@link InputFormat}, but {@link
 * DataStreamScanProvider} just produce datastream, we can not get the inner source(note this source
 * could be SourceFunction or new Source or InputFormat). But sometimes we need it, a common
 * scenario is HybridSource, we need to extract inner source from provider.
 *
 * @param <T> source type
 */
@PublicEvolving
public interface DataStreamScanSourceAbilityProvider<T> extends DataStreamScanProvider {

    /**
     * Expose DataStreamScanProvider inner source.
     *
     * @return inner source
     */
    T createSource();
}
