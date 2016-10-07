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

package org.apache.flink.api.table

import java.lang.Iterable

import org.apache.calcite.jdbc.CalciteSchema
import org.apache.calcite.plan._
import org.apache.calcite.prepare.CalciteCatalogReader
import org.apache.calcite.rel.logical.LogicalAggregate
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.schema.SchemaPlus
import org.apache.calcite.tools.Frameworks.PlannerAction
import org.apache.calcite.tools.RelBuilder.{AggCall, GroupKey}
import org.apache.calcite.tools.{FrameworkConfig, Frameworks, RelBuilder}
import org.apache.flink.api.table.FlinkRelBuilder.NamedProperty
import org.apache.flink.api.table.expressions.{NamedExpression, Property}
import org.apache.flink.api.table.plan.logical.LogicalWindow
import org.apache.flink.api.table.plan.logical.rel.LogicalWindowAggregate

/**
  * Flink specific [[RelBuilder]] that changes the default type factory to a [[FlinkTypeFactory]].
  */
class FlinkRelBuilder(
    context: Context,
    relOptCluster: RelOptCluster,
    relOptSchema: RelOptSchema)
  extends RelBuilder(
    context,
    relOptCluster,
    relOptSchema) {

  def getPlanner: RelOptPlanner = cluster.getPlanner

  def getCluster: RelOptCluster = relOptCluster

  override def getTypeFactory: FlinkTypeFactory =
    super.getTypeFactory.asInstanceOf[FlinkTypeFactory]

  def aggregate(
      window: LogicalWindow,
      groupKey: GroupKey,
      namedProperties: Seq[NamedProperty],
      aggCalls: Iterable[AggCall])
    : RelBuilder = {
    // build logical aggregate
    val aggregate = super.aggregate(groupKey, aggCalls).build().asInstanceOf[LogicalAggregate]

    // build logical window aggregate from it
    push(LogicalWindowAggregate.create(window, namedProperties, aggregate))
    this
  }

}

object FlinkRelBuilder {

  def create(config: FrameworkConfig): FlinkRelBuilder = {
    // prepare planner and collect context instances
    val clusters: Array[RelOptCluster] = Array(null)
    val relOptSchemas: Array[RelOptSchema] = Array(null)
    val rootSchemas: Array[SchemaPlus] = Array(null)
    Frameworks.withPlanner(new PlannerAction[Void] {
      override def apply(
          cluster: RelOptCluster,
          relOptSchema: RelOptSchema,
          rootSchema: SchemaPlus)
        : Void = {
        clusters(0) = cluster
        relOptSchemas(0) = relOptSchema
        rootSchemas(0) = rootSchema
        null
      }
    })
    val planner = clusters(0).getPlanner
    planner.setExecutor(config.getExecutor)
    val defaultRelOptSchema = relOptSchemas(0).asInstanceOf[CalciteCatalogReader]

    // create Flink type factory
    val typeSystem = config.getTypeSystem
    val typeFactory = new FlinkTypeFactory(typeSystem)

    // create context instances with Flink type factory
    val cluster = RelOptCluster.create(planner, new RexBuilder(typeFactory))
    val calciteSchema = CalciteSchema.from(config.getDefaultSchema)
    val relOptSchema = new CalciteCatalogReader(
      calciteSchema,
      config.getParserConfig.caseSensitive(),
      defaultRelOptSchema.getSchemaName,
      typeFactory)

    new FlinkRelBuilder(config.getContext, cluster, relOptSchema)
  }

  /**
    * Information necessary to create a window aggregate.
    *
    * Similar to [[RelBuilder.AggCall]] or [[RelBuilder.GroupKey]].
    */
  case class NamedProperty(name: String, property: Property)

}
