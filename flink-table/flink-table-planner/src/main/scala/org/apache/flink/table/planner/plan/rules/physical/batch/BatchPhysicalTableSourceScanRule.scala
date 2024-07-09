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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.connector.source.ScanTableSource
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.abilities.source.{PartitioningSpec, SourceAbilityContext, SourceAbilitySpec}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.ScanUtil
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext

import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.convert.ConverterRule.Config
import org.apache.calcite.rel.core.TableScan

/** Rule that converts [[FlinkLogicalTableSourceScan]] to [[BatchPhysicalTableSourceScan]]. */
class BatchPhysicalTableSourceScanRule(config: Config) extends ConverterRule(config) {

  /** Rule must only match if TableScan targets a bounded [[ScanTableSource]] */
  override def matches(call: RelOptRuleCall): Boolean = {
    val scan: TableScan = call.rel(0).asInstanceOf[TableScan]
    val tableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable])
    tableSourceTable match {
      case tst: TableSourceTable =>
        tst.tableSource match {
          case sts: ScanTableSource =>
            sts.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE).isBounded
          case _ => false
        }
      case _ => false
    }
  }

  private def applyPartitioning(scan: TableScan) = {
    val oldTableSourceTable = scan.getTable.unwrap(classOf[TableSourceTable])
    val newTableSource = oldTableSourceTable.tableSource.copy
    val partitioningSpec = new PartitioningSpec()
    partitioningSpec.apply(newTableSource, SourceAbilityContext.from(scan))
    val myArray: Array[SourceAbilitySpec] = Array(partitioningSpec)

    oldTableSourceTable.copy(newTableSource, oldTableSourceTable.getRowType, myArray)
  }

  def convert(rel: RelNode): RelNode = {
    val scan = rel.asInstanceOf[FlinkLogicalTableSourceScan]
    var newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val partitionKeys = ScanUtil.getPartitionKeys(scan.relOptTable)
    var scanTableSource = scan.getTable.asInstanceOf[TableSourceTable]

    if (partitionKeys.nonEmpty) {
      val partitionIdxs: Array[Int] = partitionKeys.get
      val newDistribution = FlinkRelDistribution.hash(partitionIdxs, requireStrict = true)
      newTrait = newTrait.replace(newDistribution)
      scanTableSource = applyPartitioning(scan)
    }

    new BatchPhysicalTableSourceScan(
      rel.getCluster,
      newTrait,
      scan.getHints,
      scanTableSource
    )
  }
}

object BatchPhysicalTableSourceScanRule {
  val INSTANCE: ConverterRule = new BatchPhysicalTableSourceScanRule(
    Config.INSTANCE.withConversion(
      classOf[FlinkLogicalTableSourceScan],
      FlinkConventions.LOGICAL,
      FlinkConventions.BATCH_PHYSICAL,
      "BatchPhysicalTableSourceScanRule"))
}
