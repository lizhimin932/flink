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

package org.apache.flink.api.table.plan.nodes.dataset

import org.apache.calcite.plan.{RelOptCost, RelOptPlanner, RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelWriter, BiRel, RelNode}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.DataSet
import org.apache.flink.api.table.{BatchTableEnvironment, TableConfig}

import scala.collection.JavaConverters._
import scala.collection.JavaConversions._

/**
* Flink RelNode which matches along with UnionOperator.
*
*/
class DataSetUnion(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    left: RelNode,
    right: RelNode,
    rowType: RelDataType,
    all: Boolean)
  extends BiRel(cluster, traitSet, left, right)
  with DataSetRel {

  override def deriveRowType() = rowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataSetUnion(
      cluster,
      traitSet,
      inputs.get(0),
      inputs.get(1),
      rowType,
      all
    )
  }

  private val allStr: String = if (all) "All" else ""

  override def toString: String = {
    s"Union$allStr(union: (${rowType.getFieldNames.asScala.toList.mkString(", ")}))"
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw).item(s"union$allStr", unionSelectionToString)
  }

  override def computeSelfCost (planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {

    val children = this.getInputs
    val rowCnt = children.foldLeft(0D) { (rows, child) =>
      rows + metadata.getRowCount(child)
    }

    planner.getCostFactory.makeCost(if (all) rowCnt else rowCnt * 0.1, 0, 0)
  }

  override def translateToPlan(
      tableEnv: BatchTableEnvironment,
      expectedType: Option[TypeInformation[Any]]): DataSet[Any] = {

    val leftDataSet = left.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    val rightDataSet = right.asInstanceOf[DataSetRel].translateToPlan(tableEnv)
    if (all) {
      leftDataSet.union(rightDataSet).asInstanceOf[DataSet[Any]]
    } else {
      leftDataSet.union(rightDataSet).distinct().asInstanceOf[DataSet[Any]]
    }
  }

  private def unionSelectionToString: String = {
    rowType.getFieldNames.asScala.toList.mkString(", ")
  }

}
