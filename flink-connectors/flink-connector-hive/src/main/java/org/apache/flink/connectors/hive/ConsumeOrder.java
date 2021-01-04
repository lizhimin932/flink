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

package org.apache.flink.connectors.hive;

import java.util.Arrays;

/** {@link ConsumeOrder} defines the orders to continuously consume stream source. */
public enum ConsumeOrder {

    /**
     * create-time compare partition/file creation time, this is not the partition create time in
     * Hive metaStore, but the folder/file create time in filesystem.
     */
    CREATE_TIME_ORDER("create-time"),

    /** partition-time compare time represented by partition name. */
    PARTITION_TIME_ORDER("partition-time"),

    /** partition-name compare partition names, the comparator oder is the alphabetical order. */
    PARTITION_NAME_ORDER("partition-name");

    private final String order;

    ConsumeOrder(String order) {
        this.order = order;
    }

    @Override
    public String toString() {
        return order;
    }

    /** Get {@link ConsumeOrder} from consume order string. */
    public static ConsumeOrder getConsumeOrder(String consumeOrderStr) {
        for (ConsumeOrder consumeOrder : values()) {
            if (consumeOrder.order.equalsIgnoreCase(consumeOrderStr)) {
                return consumeOrder;
            }
        }
        throw new IllegalArgumentException(
                "Illegal value: "
                        + consumeOrderStr
                        + ", only "
                        + Arrays.toString(values())
                        + " are supported.");
    }
}
