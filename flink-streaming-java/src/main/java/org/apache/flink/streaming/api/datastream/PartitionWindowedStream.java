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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.PublicEvolving;

/**
 * {@link PartitionWindowedStream} represents a data stream that collects all records of each
 * partition separately into a full window. Window emission will be triggered at the end of inputs.
 * For non-keyed {@link DataStream}, a partition contains all records of a subtask. For {@link
 * KeyedStream}, a partition contains all records of a key.
 *
 * @param <T> The type of the elements in this stream.
 */
@PublicEvolving
public interface PartitionWindowedStream<T> {}
