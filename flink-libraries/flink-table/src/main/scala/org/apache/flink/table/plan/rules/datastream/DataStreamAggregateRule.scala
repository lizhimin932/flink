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

package org.apache.flink.table.plan.rules.datastream

import org.apache.calcite.plan.{Convention, RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.flink.table.api.TableException
import org.apache.flink.table.plan.logical.rel.LogicalWindowAggregate
import org.apache.flink.table.plan.nodes.datastream.{DataStreamAggregate, DataStreamConvention, DataStreamUnion}

import scala.collection.JavaConversions._

class DataStreamAggregateRule
  extends ConverterRule(
    classOf[LogicalWindowAggregate],
    Convention.NONE,
    DataStreamConvention.INSTANCE,
    "DataStreamAggregateRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val agg: LogicalWindowAggregate = call.rel(0).asInstanceOf[LogicalWindowAggregate]

    // check if we have distinct aggregates
    val distinctAggs = agg.getAggCallList.exists(_.isDistinct)
    if (distinctAggs) {
      throw TableException("DISTINCT aggregates are currently not supported.")
    }

    !distinctAggs
  }

  override def convert(rel: RelNode): RelNode = {
    val agg: LogicalWindowAggregate = rel.asInstanceOf[LogicalWindowAggregate]
    val traitSet: RelTraitSet = rel.getTraitSet.replace(DataStreamConvention.INSTANCE)
    val convInput: RelNode = RelOptRule.convert(agg.getInput, DataStreamConvention.INSTANCE)

    if (agg.indicator) {
      agg.groupSets.map(set =>
        new DataStreamAggregate(
          agg.getWindow,
          agg.getNamedProperties,
          rel.getCluster,
          traitSet,
          convInput,
          agg.getNamedAggCalls,
          rel.getRowType,
          agg.getInput.getRowType,
          set.toArray,
          agg.indicator
        ).asInstanceOf[RelNode]
      ).reduce(
        (rel1, rel2) => {
          new DataStreamUnion(
            rel.getCluster,
            traitSet,
            rel1, rel2,
            rel.getRowType
          )
        }
      )
    } else {
      new DataStreamAggregate(
        agg.getWindow,
        agg.getNamedProperties,
        rel.getCluster,
        traitSet,
        convInput,
        agg.getNamedAggCalls,
        rel.getRowType,
        agg.getInput.getRowType,
        agg.getGroupSet.toArray,
        agg.indicator
      )
    }
  }
}

object DataStreamAggregateRule {
  val INSTANCE: RelOptRule = new DataStreamAggregateRule
}

