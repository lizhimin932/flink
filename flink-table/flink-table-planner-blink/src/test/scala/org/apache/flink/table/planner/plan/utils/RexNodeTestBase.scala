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

package org.apache.flink.table.planner.plan.utils

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.planner.calcite.{FlinkRexBuilder, FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.planner.plan.utils.InputTypeBuilder.inputOf
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode, RexProgram, RexProgramBuilder}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, INTEGER, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import java.math.BigDecimal
import java.util.{List => JList}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

abstract class RexNodeTestBase {

  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem)

  val allFieldNames: java.util.List[String] = List("name", "id", "amount", "price", "flag").asJava

  val allFieldTypes: java.util.List[RelDataType] = List(DataTypes.VARCHAR(100),
    DataTypes.BIGINT(), DataTypes.INT(), DataTypes.DOUBLE(), DataTypes.BOOLEAN())
    .map(LogicalTypeDataTypeConverter.fromDataTypeToLogicalType)
    .map(typeFactory.createFieldTypeFromLogicalType)
    .asJava

  var rexBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)

  val program: RexProgram = buildSimpleRexProgram()

  protected def buildConditionExpr(): RexNode = {
    program.expandLocalRef(program.getCondition)
  }

  protected def buildExprs(): JList[RexNode] = {
    val exprs = program.getProjectList.map(expr => program.expandLocalRef(expr))
    (exprs :+ buildConditionExpr()).asJava
  }

  // select amount, amount * price as total where amount * price < 100 and id > 6
  protected def buildSimpleRexProgram(): RexProgram = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    val t3 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t2))
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val t5 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(6L))

    // project: amount, amount * price as total
    builder.addProject(t0, "amount")
    builder.addProject(t3, "total")

    // condition: amount * price < 100 and id > 6
    val t6 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t3, t4))
    val t7 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t5))
    val t8 = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, List(t6, t7).asJava))
    builder.addCondition(t8)

    builder.getProgram
  }

  protected def makeTypes(fieldTypes: SqlTypeName*): java.util.List[RelDataType] = {
    fieldTypes.toList.map(typeFactory.createSqlType).asJava
  }

  protected def buildExprsWithDeepNesting(): JList[RexNode] = {

    // person input
    val passportRow = inputOf(typeFactory)
      .field("id", VARCHAR)
      .field("status", VARCHAR)
      .build

    val personRow = inputOf(typeFactory)
      .field("name", VARCHAR)
      .field("age", INTEGER)
      .nestedField("passport", passportRow)
      .build

    // payment input
    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    // deep field input
    val deepRowType = inputOf(typeFactory)
      .field("entry", VARCHAR)
      .build

    val entryRowType = inputOf(typeFactory)
      .nestedField("inside", deepRowType)
      .build

    val deeperRowType = inputOf(typeFactory)
      .nestedField("entry", entryRowType)
      .build

    val withRowType = inputOf(typeFactory)
      .nestedField("deep", deepRowType)
      .nestedField("deeper", deeperRowType)
      .build

    val fieldRowType = inputOf(typeFactory)
      .nestedField("with", withRowType)
      .build

    // inputRowType
    //
    // [ persons:  [ name: VARCHAR, age:  INT, passport: [id: VARCHAR, status: VARCHAR ] ],
    //   payments: [ id: BIGINT, amount: INT ],
    //   field:    [ with: [ deep: [ entry: VARCHAR ],
    //                       deeper: [ entry: [ inside: [entry: VARCHAR ] ] ]
    //             ] ]
    // ]

    val t0 = rexBuilder.makeInputRef(personRow, 0)
    val t1 = rexBuilder.makeInputRef(paymentRow, 1)
    val t2 = rexBuilder.makeInputRef(fieldRowType, 2)
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10L))

    // person
    val person$pass = rexBuilder.makeFieldAccess(t0, "passport", false)
    val person$pass$stat = rexBuilder.makeFieldAccess(person$pass, "status", false)

    // payment
    val pay$amount = rexBuilder.makeFieldAccess(t1, "amount", false)
    val multiplyAmount = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, pay$amount, t3)

    // field
    val field$with = rexBuilder.makeFieldAccess(t2, "with", false)
    val field$with$deep = rexBuilder.makeFieldAccess(field$with, "deep", false)
    val field$with$deeper = rexBuilder.makeFieldAccess(field$with, "deeper", false)
    val field$with$deep$entry = rexBuilder.makeFieldAccess(field$with$deep, "entry", false)
    val field$with$deeper$entry = rexBuilder.makeFieldAccess(field$with$deeper, "entry", false)
    val field$with$deeper$entry$inside = rexBuilder
      .makeFieldAccess(field$with$deeper$entry, "inside", false)
    val field$with$deeper$entry$inside$entry = rexBuilder
      .makeFieldAccess(field$with$deeper$entry$inside, "entry", false)

    // Program
    // (
    //   payments.amount * 10),
    //   persons.passport.status,
    //   field.with.deep.entry
    //   field.with.deeper.entry.inside.entry
    //   field.with.deeper.entry
    //   persons
    // )
    List(multiplyAmount, person$pass$stat, field$with$deep$entry,
      field$with$deeper$entry$inside$entry, field$with$deeper$entry, t0).asJava

  }

  protected def buildExprsWithNesting(): JList[RexNode] = {
    val personRow = inputOf(typeFactory)
      .field("name", INTEGER)
      .field("age", VARCHAR)
      .build

    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    val types = List(personRow, paymentRow).asJava

    val t0 = rexBuilder.makeInputRef(types.get(0), 0)
    val t1 = rexBuilder.makeInputRef(types.get(1), 1)
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val payment$amount = rexBuilder.makeFieldAccess(t1, "amount", false)

    List(payment$amount, t0, t2).asJava
  }

}
