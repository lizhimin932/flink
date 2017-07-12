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
package org.apache.flink.table.expressions.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.{Ignore, Test}

class ExpressionReductionTest extends TableTestBase {

  @Test
  def testReduceCalcExpression(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .where('a > (1 + 7))
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.days + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select",
        "13 AS _c0",
        "'b' AS _c1",
        "'STRING' AS _c2",
        "'teststring' AS _c3",
        "1990-10-24 23:00:01.123 AS _c4",
        "false AS _c5",
        "true AS _c6",
        "2E0 AS _c7",
        "'trueX' AS _c8"
      ),
      term("where", ">(a, 8)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testReduceProjectExpression(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result =  table
      .select((3 + 4).toExpr + 6,
              (11 === 1) ? ("a", "b"),
              " STRING ".trim,
              "test" + "string",
              "1990-10-14 23:00:00.123".toTimestamp + 10.days + 1.second,
              1.isNull,
              "TEST".like("%EST"),
              2.5.toExpr.floor(),
              true.cast(Types.STRING) + "X")

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select",
        "13 AS _c0",
        "'b' AS _c1",
        "'STRING' AS _c2",
        "'teststring' AS _c3",
        "1990-10-24 23:00:01.123 AS _c4",
        "false AS _c5",
        "true AS _c6",
        "2E0 AS _c7",
        "'trueX' AS _c8"
      )
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testReduceFilterExpression(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .where('a > (1 + 7))

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a", "b", "c"),
      term("where", ">(a, 8)")
    )

    util.verifyTable(result, expected)
  }

  @Test
  def testNestedTablesReduction(): Unit = {
    val util = streamTestUtil()

    util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val newTable = util.tableEnv.sql("SELECT 1 + 1 + a AS a FROM MyTable")

    util.tableEnv.registerTable("NewTable", newTable)

    val sqlQuery = "SELECT a FROM NewTable"

    // 1+1 should be normalized to 2
    val expected = unaryNode("DataStreamCalc", streamTableNode(0), term("select", "+(2, a) AS a"))

    util.verifySql(sqlQuery, expected)
  }
  // todo this NPE is caused by Calcite, it shall pass when [CALCITE-1860] is fixed
  @Ignore
  def testReduceDeterministicUDF(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    // if isDeterministic = true, will cause a Calcite NPE, which will be fixed in [CALCITE-1860]
    val result = table
      .select('a, 'b, 'c, DeterministicNullFunc() as 'd)
      .where("d.isNull")
      .select('a, 'b, 'c)

    val expected: String = streamTableNode(0)

    util.verifyTable(result, expected)
  }

  @Test
  def testReduceNonDeterministicUDF(): Unit = {
    val util = streamTestUtil()
    val table = util.addTable[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

    val result = table
      .select('a, 'b, 'c, NonDeterministicNullFunc() as 'd)
      .where("d.isNull")
      .select('a, 'b, 'c)

    val expected = unaryNode(
      "DataStreamCalc",
      streamTableNode(0),
      term("select", "a", "b", "c"),
      term("where", s"IS NULL(${NonDeterministicNullFunc.functionIdentifier}())")
    )

    util.verifyTable(result, expected)
  }

}

object NonDeterministicNullFunc extends ScalarFunction {
  def eval(): String = null
  override def isDeterministic = false
}

object DeterministicNullFunc extends ScalarFunction {
  def eval(): String = null
  override def isDeterministic = true
}
