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

package org.apache.flink.table.plan.nodes.physical.stream

import org.apache.flink.streaming.api.transformations.{OneInputTransformation, StreamTransformation}
import org.apache.flink.table.api.{StreamTableEnvironment, TableConfig, TableConfigOptions,
TableException}
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.codegen.sort.SortCodeGenerator
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.plan.util.{RelExplainUtil, SortUtil}
import org.apache.flink.table.runtime.keyselector.NullBinaryRowKeySelector
import org.apache.flink.table.runtime.sort.{OnlyRowTimeSortOperator, ProcTimeSortOperator, RowTimeSortOperator}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.calcite.rel._
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexNode

import java.util

import scala.collection.JavaConversions._

/**
  * Stream physical RelNode for time-ascending-order [[Sort]] without `limit`.
  *
  * @see [[StreamExecRank]] which must be with `limit` order by.
  * @see [[StreamExecSort]] which can be used for testing now, its sort key can be any type.
  */
class StreamExecTemporalSort(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    sortCollation: RelCollation)
  extends Sort(cluster, traitSet, inputRel, sortCollation)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = false

  override def needsUpdatesAsRetraction(input: RelNode): Boolean = false

  override def consumesRetractions: Boolean = false

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      input: RelNode,
      newCollation: RelCollation,
      offset: RexNode,
      fetch: RexNode): Sort = {
    new StreamExecTemporalSort(cluster, traitSet, input, newCollation)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.input("input", getInput())
      .item("orderBy", RelExplainUtil.collationToString(sortCollation, getRowType))
  }

  //~ ExecNode methods -----------------------------------------------------------

  /**
    * Returns an array of this node's inputs. If there are no inputs,
    * returns an empty list, not null.
    *
    * @return Array of this node's inputs
    */
  override def getInputNodes: util.List[ExecNode[StreamTableEnvironment, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamTableEnvironment, _]])
  }

  /**
    * Internal method, translates this node into a Flink operator.
    *
    * @param tableEnv The [[StreamTableEnvironment]] of the translated Table.
    */
  override protected def translateToPlanInternal(
      tableEnv: StreamTableEnvironment): StreamTransformation[BaseRow] = {

    // time ordering needs to be ascending
    if (SortUtil.getFirstSortDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException(
        "Sort: Primary sort order of a streaming table must be ascending on time.\n" +
          "please re-check sort statement according to the description above")
    }

    val input = getInputNodes.get(0).translateToPlan(tableEnv)
      .asInstanceOf[StreamTransformation[BaseRow]]

    val timeType = SortUtil.getFirstSortField(sortCollation, getRowType).getType
    timeType match {
      case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
        createSortProcTime(input, tableEnv.getConfig)
      case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        createSortRowTime(input, tableEnv.getConfig)
      case _ =>
        throw new TableException(
          "Sort: Internal Error\n" +
            "Normally, this happens unlikely. please contact customer support for this"
        )
    }
  }

  /**
    * Create Sort logic based on processing time
    */
  private def createSortProcTime(
      input: StreamTransformation[BaseRow],
      tableConfig: TableConfig): StreamTransformation[BaseRow] = {

    val inputType = FlinkTypeFactory.toInternalRowType(getInput.getRowType)

    // if the order has secondary sorting fields in addition to the proctime
    if (sortCollation.getFieldCollations.size() > 1) {

      // strip off time collation
      val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
        sortCollation.getFieldCollations.tail)

      // sort code gen
      val keyTypes = keys.map(inputType.getTypeAt)
      val codeGen = new SortCodeGenerator(tableConfig, keys, keyTypes, orders, nullsIsLast)

      val memorySize = tableConfig.getConf.getInteger(
        TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM) * TableConfigOptions.SIZE_IN_MB

      val sortOperator = new ProcTimeSortOperator(
        inputType.toTypeInfo,
        memorySize,
        codeGen.generateNormalizedKeyComputer("ProcTimeSortComputer"),
        codeGen.generateRecordComparator("ProcTimeSortComparator"))

      val outputRowTypeInfo = FlinkTypeFactory.toInternalRowType(getRowType).toTypeInfo
      val ret = new OneInputTransformation(
        input, "ProcTimeSortOperator", sortOperator, outputRowTypeInfo, input.getParallelism)
      val selector = NullBinaryRowKeySelector.INSTANCE
      ret.setStateKeySelector(selector)
      ret.setStateKeyType(selector.getProducedType)
      ret
    } else {
      // if the order is done only on proctime we only need to forward the elements
      input
    }
  }

  /**
    * Create Sort logic based on row time
    */
  private def createSortRowTime(
      input: StreamTransformation[BaseRow],
      tableConfig: TableConfig): StreamTransformation[BaseRow] = {
    val rowtimeIdx = sortCollation.getFieldCollations.get(0).getFieldIndex

    val inputType = FlinkTypeFactory.toInternalRowType(getInput.getRowType)

    val sortOperator = if (sortCollation.getFieldCollations.size() > 1) {
      // strip off time collation
      val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
        sortCollation.getFieldCollations.tail)

      // sort code gen
      val keyTypes = keys.map(inputType.getTypeAt)
      val codeGen = new SortCodeGenerator(tableConfig, keys, keyTypes, orders, nullsIsLast)

      val memorySize = tableConfig.getConf.getInteger(
        TableConfigOptions.SQL_RESOURCE_SORT_BUFFER_MEM) * TableConfigOptions.SIZE_IN_MB

      new RowTimeSortOperator(
        inputType.toTypeInfo,
        memorySize,
        rowtimeIdx,
        codeGen.generateNormalizedKeyComputer("RowTimeSortComputer"),
        codeGen.generateRecordComparator("RowTimeSortComparator"))
    } else {
      new OnlyRowTimeSortOperator(inputType.toTypeInfo, rowtimeIdx)
    }

    val outputRowTypeInfo = FlinkTypeFactory.toInternalRowType(getRowType).toTypeInfo

    val ret = new OneInputTransformation(
      input, "RowTimeSortOperator", sortOperator, outputRowTypeInfo, input.getParallelism)
    val selector = NullBinaryRowKeySelector.INSTANCE
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)
    ret
  }
}
