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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.connector.source.abilities.SupportsAggregatePushDown;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalGroupAggregateBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalLocalSortAggregate;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSort;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;

import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Planner rule that tries to push a local sort aggregate which with sort into a {@link
 * BatchPhysicalTableSourceScan} which table is a {@link TableSourceTable}. And the table source in
 * the table is a {@link SupportsAggregatePushDown}.
 *
 * <p>When the {@code OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED} is
 * true, we have the original physical plan:
 *
 * <pre>{@code
 * BatchPhysicalSortAggregate (global)
 * +- Sort (exists if group keys are not empty)
 *    +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *       +- BatchPhysicalLocalSortAggregate (local)
 *          +- Sort (exists if group keys are not empty)
 *             +- BatchPhysicalTableSourceScan
 * }</pre>
 *
 * <p>This physical plan will be rewritten to:
 *
 * <pre>{@code
 * BatchPhysicalSortAggregate (global)
 * +- Sort (exists if group keys are not empty)
 *    +- BatchPhysicalExchange (hash by group keys if group keys is not empty, else singleton)
 *       +- BatchPhysicalTableSourceScan (with local aggregate pushed down)
 * }</pre>
 */
public class PushLocalAggWithSortIntoTableSourceScanRule
        extends PushLocalAggIntoTableSourceScanRuleBase {
    public static final PushLocalAggWithSortIntoTableSourceScanRule INSTANCE =
            new PushLocalAggWithSortIntoTableSourceScanRule();

    public PushLocalAggWithSortIntoTableSourceScanRule() {
        super(
                operand(
                        BatchPhysicalExchange.class,
                        operand(
                                BatchPhysicalLocalSortAggregate.class,
                                operand(
                                        BatchPhysicalSort.class,
                                        operand(BatchPhysicalTableSourceScan.class, none())))),
                "PushLocalAggWithSortIntoTableSourceScanRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        BatchPhysicalGroupAggregateBase localAggregate = call.rel(1);
        BatchPhysicalTableSourceScan tableSourceScan = call.rel(3);
        return isMatch(call, localAggregate, tableSourceScan);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        BatchPhysicalGroupAggregateBase localSortAgg = call.rel(1);
        BatchPhysicalTableSourceScan oldScan = call.rel(3);
        pushLocalAggregateIntoScan(call, localSortAgg, oldScan);
    }
}
