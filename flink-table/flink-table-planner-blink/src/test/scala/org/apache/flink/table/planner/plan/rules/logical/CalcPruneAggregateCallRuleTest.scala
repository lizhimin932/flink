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
package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets

/**
  * Test for [[PruneAggregateCallRule]]#CALC_ON_AGGREGATE.
  */
class CalcPruneAggregateCallRuleTest extends PruneAggregateCallRuleTestBase {

  override def setup(): Unit = {
    super.setup()
    util.buildBatchProgram(FlinkBatchProgram.LOGICAL)
    val programs = util.getBatchProgram()
    programs.addLast("rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
      .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
      .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
      .add(RuleSets.ofList(
        AggregateReduceGroupingRule.INSTANCE,
        CoreRules.FILTER_CALC_MERGE,
        CoreRules.PROJECT_CALC_MERGE,
        CoreRules.FILTER_TO_CALC,
        CoreRules.PROJECT_TO_CALC,
        FlinkCalcMergeRule.INSTANCE,
        PruneAggregateCallRule.CALC_ON_AGGREGATE)
      ).build())
    util.replaceBatchProgram(programs)
  }
}
