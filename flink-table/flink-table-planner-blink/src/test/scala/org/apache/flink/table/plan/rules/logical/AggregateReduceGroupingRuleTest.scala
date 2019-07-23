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
package org.apache.flink.table.plan.rules.logical

import org.apache.flink.table.plan.common.AggregateReduceGroupingTestBase
import org.apache.flink.table.planner.plan.optimize.program.FlinkBatchProgram
import org.apache.flink.table.planner.plan.rules.logical.FlinkAggregateRemoveRule

import org.apache.calcite.tools.RuleSets
import org.junit.Before

/**
  * Test for [[AggregateReduceGroupingRule]].
  */
class AggregateReduceGroupingRuleTest extends AggregateReduceGroupingTestBase {

  @Before
  override def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.LOGICAL_REWRITE)

    // remove FlinkAggregateRemoveRule to prevent the agg from removing
    val programs = util.getBatchProgram()
    programs.getFlinkRuleSetProgram(FlinkBatchProgram.LOGICAL).get
      .remove(RuleSets.ofList(FlinkAggregateRemoveRule.INSTANCE))
    util.replaceBatchProgram(programs)

    super.setup()
  }
}

