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

package org.apache.flink.table.connector.source.abilities;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.data.RowData;

import java.io.Serializable;
import java.util.Optional;

/**
 * {@link SupportsLookupCustomShuffle} is designed to allow connectors to provide a custom
 * partitioning strategy for the data that is fed into the {@link LookupTableSource}. This enables
 * the Flink Planner to optimize the distribution of input stream across different subtasks of
 * lookup-join node to match the distribution of data in the external data source.
 */
@PublicEvolving
public interface SupportsLookupCustomShuffle {

    /**
     * This method is used to retrieve a custom partitioner that will be applied to the input stream
     * of lookup-join node.
     *
     * @return An {@link InputDataPartitioner} that defines how records should be distributed across
     *     the different subtasks. If the connector expects the input data to remain in its original
     *     distribution, an {@link Optional#empty()} should be returned.
     */
    Optional<InputDataPartitioner> getPartitioner();

    /**
     * This interface is responsible for providing custom partitioning logic for the RowData
     * records.
     */
    @PublicEvolving
    interface InputDataPartitioner extends Serializable {

        /**
         * Determining the partition id for each input data.
         *
         * <p>This data is projected to only including all join keys before emit to this
         * partitioner.
         *
         * @param joinKeys The extracted join keys for each input record.
         * @param numPartitions The total number of partition.
         * @return An integer representing the partition id to which the record should be sent.
         */
        int partition(RowData joinKeys, int numPartitions);
    }
}
