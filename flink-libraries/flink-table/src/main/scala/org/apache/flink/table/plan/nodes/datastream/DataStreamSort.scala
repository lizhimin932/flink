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

package org.apache.flink.table.plan.nodes.datastream

import org.apache.calcite.plan.{ RelOptCluster, RelTraitSet }
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.{ RelNode, RelWriter, SingleRel }
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.datastream.{ AllWindowedStream, DataStream, KeyedStream, WindowedStream }
import org.apache.flink.streaming.api.windowing.assigners._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{ Window => DataStreamWindow }
import org.apache.flink.table.calcite.FlinkRelBuilder.NamedWindowProperty
import org.apache.flink.table.calcite.FlinkTypeFactory
import org.apache.flink.table.expressions._
import org.apache.flink.table.plan.logical._
import org.apache.flink.table.plan.nodes.CommonAggregate
import org.apache.flink.table.runtime.aggregate.AggregateUtil._
import org.apache.flink.table.runtime.aggregate._
import org.apache.flink.table.typeutils.TypeCheckUtils.isTimeInterval
import org.apache.flink.table.typeutils.{ RowIntervalTypeInfo, TimeIntervalTypeInfo }
import org.apache.flink.types.Row
import org.apache.calcite.sql.SqlAggFunction
import org.apache.flink.table.plan.nodes.datastream.DataStreamRel
import org.apache.calcite.sql.fun.SqlSingleValueAggFunction
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.util.Collector
import org.apache.flink.api.common.state.ValueState
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.calcite.rel.core.Sort
import org.apache.flink.api.java.functions.NullByteKeySelector
import org.apache.calcite.rel.RelFieldCollation.Direction
import org.apache.flink.table.runtime.aggregate.SortUtil._
import org.apache.calcite.rel.RelCollation
import org.apache.calcite.rex.RexNode
import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.table.runtime.types.{CRow, CRowTypeInfo}
import org.apache.flink.table.api.{StreamQueryConfig, StreamTableEnvironment, TableException}
import org.apache.flink.table.plan.schema.RowSchema

/**
  * Flink RelNode which matches along with Sort Rule.
  *
 */
class DataStreamSort(
  sortCollation: RelCollation,
  sortOffset: RexNode,
  sortFetch: RexNode,
  cluster: RelOptCluster,
  traitSet: RelTraitSet,
  inputNode: RelNode,
  schema: RowSchema,
  inputSchema: RowSchema,
  description: String)
    extends SingleRel(cluster, traitSet, inputNode) with DataStreamRel {

  override def deriveRowType(): RelDataType = schema.logicalType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new DataStreamSort(
      sortCollation,
      sortOffset,
      sortFetch,
      cluster,
      traitSet,
      inputs.get(0),
      schema,
      inputSchema,
      description)
  }

  override def toString: String = {
    s"Sort(by: ($SortUtil.getSortFieldToString(sortCollation, rowRelDataType))," +
      " offset: $SortUtil.getOffsetToString(sortOffset)," +
      " fetch: $SortUtil.getFetchToString(sortFetch, sortOffset))"
  }
  
  override def explainTerms(pw: RelWriter) : RelWriter = {
    
    //need to identify time between others order fields. Time needs to be first sort element
    checkTimeOrder()
    
    super.explainTerms(pw)
      .item("orderBy", SortUtil.getSortFieldToString(sortCollation, schema.logicalType))
      .item("offset", SortUtil.getOffsetToString(sortOffset))
      .item("fetch", SortUtil.getFetchToString(sortFetch, sortOffset))
  }

  override def translateToPlan(
      tableEnv: StreamTableEnvironment,
      queryConfig: StreamQueryConfig): DataStream[CRow] = {
    
    val inputDS = input.asInstanceOf[DataStreamRel].translateToPlan(tableEnv, queryConfig)
    
    //need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getTimeType(sortCollation, schema.logicalType)
    
    //time ordering needs to be ascending
    if (SortUtil.getTimeDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException("SQL/Table supports only ascending time ordering")
    }
    
    val execCfg = tableEnv.execEnv.getConfig
      
    //enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType)  =>
            (sortOffset,sortFetch) match {
              case (o: Any, f: Any)  => // offset and fetch needs retraction
                throw new TableException("SQL/Table does not support sort with offset and fetch") 
              case (_, f: Any) => // offset needs retraction
                throw new TableException("SQL/Table does not support sort with fetch")
              case (o: Any, _) =>  // fetch needs retraction
                throw new TableException("SQL/Table does not support sort with offset")
              case _ => createSortProcTime(inputDS, execCfg)  //sort can be done without retraction
            }
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
            (sortOffset,sortFetch) match {
              case (o: Any, f: Any)  => // offset and fetch needs retraction
                throw new TableException("SQL/Table does not support sort with offset and fetch") 
              case (_, f: Any) => // offset needs retraction
                throw new TableException("SQL/Table does not support sort with fetch")
              case (o: Any, _) =>  // fetch needs retraction
                throw new TableException("SQL/Table does not support sort with offset")
              case _ => createSortRowTime(inputDS, execCfg)  //sort can be done without retraction
            }
        case _ =>
          throw new TableException("SQL/Table needs to have sort on time as first sort element")
    }
    
  }

  /**
   * Create Sort logic based on processing time
   */
  def createSortProcTime(
    inputDS: DataStream[CRow],
    execCfg: ExecutionConfig): DataStream[CRow] = {

   val returnTypeInfo = CRowTypeInfo(schema.physicalTypeInfo)
    
    //if the order has secondary sorting fields in addition to the proctime
    if( SortUtil.getSortFieldIndexList(sortCollation).size > 1) {
    
      val processFunction = SortUtil.createProcTimeSortFunction(sortCollation,
           inputSchema.logicalType, inputSchema.physicalTypeInfo, execCfg)
      
      inputDS
            .keyBy(new NullByteKeySelector[CRow])
            .process(processFunction).setParallelism(1).setMaxParallelism(1)
            .returns(returnTypeInfo)
            .asInstanceOf[DataStream[CRow]]
    } else {
      //if the order is done only on proctime we only need to forward the elements
        inputDS.keyBy(new NullByteKeySelector[CRow])
          .map(new IdentityRowMap())
          .setParallelism(1).setMaxParallelism(1)
          .returns(returnTypeInfo)
          .asInstanceOf[DataStream[CRow]]
    }   
  }
  
   /**
   * Create Sort logic based on row time
   */
  def createSortRowTime(
    inputDS: DataStream[CRow],
    execCfg: ExecutionConfig): DataStream[CRow] = {

    val returnTypeInfo = CRowTypeInfo(schema.physicalTypeInfo)
       
    val processFunction = SortUtil.createRowTimeSortFunction(sortCollation,
           inputSchema.logicalType, inputSchema.physicalTypeInfo, execCfg)
      
    inputDS
          .keyBy(new NullByteKeySelector[CRow])
          .process(processFunction).setParallelism(1).setMaxParallelism(1)
          .returns(returnTypeInfo)
          .asInstanceOf[DataStream[CRow]]
       
  }
  
  /**
   * Function is used to check at verification time if the SQL syntax is supported
   */
  
  def checkTimeOrder() = {
     //need to identify time between others order fields. Time needs to be first sort element
    val timeType = SortUtil.getTimeType(sortCollation, schema.logicalType)
    //time ordering needs to be ascending
    if (SortUtil.getTimeDirection(sortCollation) != Direction.ASCENDING) {
      throw new TableException("SQL/Table supports only ascending time ordering")
    }
    //enable to extend for other types of aggregates that will not be implemented in a window
    timeType match {
        case _ if FlinkTypeFactory.isProctimeIndicatorType(timeType) =>
        case _ if FlinkTypeFactory.isRowtimeIndicatorType(timeType) =>
        case _ =>
          throw new TableException("SQL/Table needs to have sort on time as first sort element")    

    }
  }

}


