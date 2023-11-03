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

package org.apache.flink.configuration;

import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.apache.flink.configuration.ClusterOptions.EVENLY_SPREAD_OUT_SLOTS_STRATEGY;
import static org.apache.flink.configuration.TaskManagerOptions.TASK_MANAGER_LOAD_BALANCE_MODE;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link TaskManagerLoadBalanceMode}. */
@ExtendWith(TestLoggerExtension.class)
class TaskManagerLoadBalanceModeTest {

    @Test
    void testReadTaskManagerLoadBalanceMode() {
        Configuration conf = new Configuration();

        // Check for 'taskmanager.load-balance.mode: NONE' and 'cluster.evenly-spread-out-slots:
        // false'
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf))
                .isEqualTo(TaskManagerLoadBalanceMode.NONE);

        // Check for 'taskmanager.load-balance.mode: NONE' and 'cluster.evenly-spread-out-slots:
        // true'
        conf.setString(EVENLY_SPREAD_OUT_SLOTS_STRATEGY.key(), "true");
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);

        // Check for 'taskmanager.load-balance.mode: SLOTS' and 'cluster.evenly-spread-out-slots:
        // false'
        conf.setString(
                TASK_MANAGER_LOAD_BALANCE_MODE.key(), TaskManagerLoadBalanceMode.SLOTS.name());
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);

        // Check for 'taskmanager.load-balance.mode: SLOTS' and 'cluster.evenly-spread-out-slots:
        // true'
        conf.setString(EVENLY_SPREAD_OUT_SLOTS_STRATEGY.key(), "true");
        assertThat(TaskManagerLoadBalanceMode.loadFromConfiguration(conf))
                .isEqualTo(TaskManagerLoadBalanceMode.SLOTS);
    }
}
