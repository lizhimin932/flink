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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

import java.sql.Timestamp

/**
  * Test for [[LogicalUnnestRule]].
  */
class LogicalUnnestRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(LogicalUnnestRule.INSTANCE))
        .build()
    )
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchProgram(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)
  }

  @Test
  def testUnnestPrimitiveArrayFromTable(): Unit = {
    util.addTableSource[(Int, Array[Int], Array[Array[Int]])]("MyTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT a, b, s FROM MyTable, UNNEST(MyTable.b) AS A (s)")
  }

  @Test
  def testUnnestArrayOfArrayFromTable(): Unit = {
    util.addTableSource[(Int, Array[Int], Array[Array[Int]])]("MyTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT a, s FROM MyTable, UNNEST(MyTable.c) AS A (s)")
  }

  @Test
  def testUnnestObjectArrayFromTableWithFilter(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    util.verifyPlan("SELECT a, b, s, t FROM MyTable, UNNEST(MyTable.b) AS A (s, t) WHERE s > 13")
  }

  @Test
  def testUnnestMultiSetFromCollectResult(): Unit = {
    util.addDataStream[(Int, Int, (Int, String))]("MyTable", 'a, 'b, 'c)
    val sqlQuery =
      """
        |WITH T AS (SELECT b, COLLECT(c) as `set` FROM MyTable GROUP BY b)
        |SELECT b, id, point FROM T, UNNEST(T.`set`) AS A(id, point) WHERE b < 3
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftUnnestMultiSetFromCollectResult(): Unit = {
    util.addDataStream[(Int, String, String)]("MyTable", 'a, 'b, 'c)
    val sqlQuery =
      """
        |WITH T AS (SELECT a, COLLECT(b) as `set` FROM MyTable GROUP BY a)
        |SELECT a, s FROM T LEFT JOIN UNNEST(T.`set`) AS A(s) ON TRUE WHERE a < 5
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTumbleWindowAggregateWithCollectUnnest(): Unit = {
    util.addDataStream[(Int, Long, String, Timestamp)]("MyTable", 'a, 'b, 'c, 'rowtime)
    val sqlQuery =
      """
        |WITH T AS (SELECT b, COLLECT(b) as `set`
        |    FROM MyTable
        |    GROUP BY b, TUMBLE(rowtime, INTERVAL '3' SECOND)
        |)
        |SELECT b, s FROM T, UNNEST(T.`set`) AS A(s) where b < 3
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCrossWithUnnest(): Unit = {
    util.addTableSource[(Int, Long, Array[String])]("MyTable", 'a, 'b, 'c)
    util.verifyPlan("SELECT a, s FROM MyTable, UNNEST(MyTable.c) as A (s)")
  }

  @Test
  def testCrossWithUnnestForMap(): Unit = {
    util.addTableSource("MyTable",
      Array[TypeInformation[_]](Types.INT,
        Types.LONG,
        Types.MAP(Types.STRING, Types.STRING)),
      Array("a", "b", "c"))
    util.verifyPlan("SELECT a, b, v FROM MyTable CROSS JOIN UNNEST(c) as f(k, v)")
  }

  @Test
  def testJoinWithUnnestOfTuple(): Unit = {
    util.addTableSource[(Int, Array[(Int, String)])]("MyTable", 'a, 'b)
    val sqlQuery =
      """
        |SELECT a, b, x, y FROM
        |    (SELECT a, b FROM MyTable WHERE a < 3) as tf,
        |    UNNEST(tf.b) as A (x, y)
        |WHERE x > a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}
