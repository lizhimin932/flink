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
package org.apache.flink.table.api.stream.sql

import org.apache.calcite.rel.logical.LogicalJoin
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.logical.TumblingGroupWindow
import org.apache.flink.table.runtime.join.WindowJoinUtil
import org.apache.flink.table.utils.TableTestUtil.{term, _}
import org.apache.flink.table.utils.{StreamTableTestUtil, TableTestBase}
import org.junit.Assert._
import org.junit.Test

class JoinTest extends TableTestBase {
  private val streamUtil: StreamTableTestUtil = streamTestUtil()
  streamUtil.addTable[(Int, String, Long)]("MyTable", 'a, 'b, 'c.rowtime, 'proctime.proctime)
  streamUtil.addTable[(Int, String, Long)]("MyTable2", 'a, 'b, 'c.rowtime, 'proctime.proctime)

  @Test
  def testProcessingTimeInnerJoinWithOnClause(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "proctime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "proctime")
          ),
          term("where",
            "AND(=(a, a0), >=(proctime, -(proctime0, 3600000)), " +
              "<=(proctime, DATETIME_PLUS(proctime0, 3600000)))"),
          term("join", "a, proctime, a0, b, proctime0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testRowTimeInnerJoinWithOnClause(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1 JOIN MyTable2 t2 ON
        |  t1.a = t2.a AND
        |  t1.c BETWEEN t2.c - INTERVAL '10' SECOND AND t2.c + INTERVAL '1' HOUR
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "c")
          ),
          term("where",
            "AND(=(a, a0), >=(c, -(c0, 10000)), " +
              "<=(c, DATETIME_PLUS(c0, 3600000)))"),
          term("join", "a, c, a0, b, c0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testProcessingTimeInnerJoinWithWhereClause(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.proctime BETWEEN t2.proctime - INTERVAL '1' HOUR AND t2.proctime + INTERVAL '1' HOUR
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "proctime")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "proctime")
          ),
          term("where",
            "AND(=(a, a0), >=(proctime, -(proctime0, 3600000)), " +
              "<=(proctime, DATETIME_PLUS(proctime0, 3600000)))"),
          term("join", "a, proctime, a0, b, proctime0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b0 AS b")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testRowTimeInnerJoinWithWhereClause(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.c BETWEEN t2.c - INTERVAL '10' MINUTE AND t2.c + INTERVAL '1' HOUR
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamCalc",
        binaryNode(
          "DataStreamWindowJoin",
          unaryNode(
            "DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c")
          ),
          unaryNode(
            "DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "c")
          ),
          term("where",
            "AND(=(a, a0), >=(c, -(c0, 600000)), " +
              "<=(c, DATETIME_PLUS(c0, 3600000)))"),
          term("join", "a, c, a0, b, c0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b0 AS b")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testJoinWithEuqiProcTime(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |t1.proctime = t2.proctime
        |""".stripMargin

    val expected =
      unaryNode("DataStreamCalc",
        binaryNode("DataStreamWindowJoin",
          unaryNode("DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "proctime")
          ),
          unaryNode("DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "proctime")
          ),
          term("where", "AND(=(a, a0), =(proctime, proctime0))"),
          term("join", "a", "proctime", "a0", "b", "proctime0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b0 AS b")
      )
    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testJoinWithEuqiRowTime(): Unit = {
    val sqlQuery =
      """
        |SELECT t1.a, t2.b
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |t1.c = t2.c
        |""".stripMargin

    val expected =
      unaryNode("DataStreamCalc",
        binaryNode("DataStreamWindowJoin",
          unaryNode("DataStreamCalc",
            streamTableNode(0),
            term("select", "a", "c")
          ),
          unaryNode("DataStreamCalc",
            streamTableNode(1),
            term("select", "a", "b", "c")
          ),
          term("where", "AND(=(a, a0), =(c, c0))"),
          term("join", "a", "c", "a0", "b", "c0"),
          term("joinType", "InnerJoin")
        ),
        term("select", "a", "b0 AS b")
      )
    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testRowTimeInnerJoinAndWindowAggregationOnFirst(): Unit = {

    val sqlQuery =
      """
        |SELECT t1.b, SUM(t2.a) AS aSum, COUNT(t2.b) AS bCnt
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.c BETWEEN t2.c - INTERVAL '10' MINUTE AND t2.c + INTERVAL '1' HOUR
        |GROUP BY TUMBLE(t1.c, INTERVAL '6' HOUR), t1.b
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          binaryNode(
            "DataStreamWindowJoin",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(0),
              term("select", "a", "b", "c")
            ),
            unaryNode(
              "DataStreamCalc",
              streamTableNode(1),
              term("select", "a", "b", "c")
            ),
            term("where",
              "AND(=(a, a0), >=(c, -(c0, 600000)), " +
                "<=(c, DATETIME_PLUS(c0, 3600000)))"),
            term("join", "a, b, c, a0, b0, c0"),
            term("joinType", "InnerJoin")
          ),
          term("select", "c", "b", "a0", "b0")
        ),
        term("groupBy", "b"),
        term("window", TumblingGroupWindow('w$, 'c, 21600000.millis)),
        term("select", "b", "SUM(a0) AS aSum", "COUNT(b0) AS bCnt")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testRowTimeInnerJoinAndWindowAggregationOnSecond(): Unit = {

    val sqlQuery =
      """
        |SELECT t2.b, SUM(t1.a) AS aSum, COUNT(t1.b) AS bCnt
        |FROM MyTable t1, MyTable2 t2
        |WHERE t1.a = t2.a AND
        |  t1.c BETWEEN t2.c - INTERVAL '10' MINUTE AND t2.c + INTERVAL '1' HOUR
        |GROUP BY TUMBLE(t2.c, INTERVAL '6' HOUR), t2.b
        |""".stripMargin

    val expected =
      unaryNode(
        "DataStreamGroupWindowAggregate",
        unaryNode(
          "DataStreamCalc",
          binaryNode(
            "DataStreamWindowJoin",
            unaryNode(
              "DataStreamCalc",
              streamTableNode(0),
              term("select", "a", "b", "c")
            ),
            unaryNode(
              "DataStreamCalc",
              streamTableNode(1),
              term("select", "a", "b", "c")
            ),
            term("where",
              "AND(=(a, a0), >=(c, -(c0, 600000)), " +
                "<=(c, DATETIME_PLUS(c0, 3600000)))"),
            term("join", "a, b, c, a0, b0, c0"),
            term("joinType", "InnerJoin")
          ),
          term("select", "c0", "b0", "a", "b")
        ),
        term("groupBy", "b0"),
        term("window", TumblingGroupWindow('w$, 'c0, 21600000.millis)),
        term("select", "b0", "SUM(a) AS aSum", "COUNT(b) AS bCnt")
      )

    streamUtil.verifySql(sqlQuery, expected)
  }

  @Test
  def testJoinTimeBoundary(): Unit = {
    verifyTimeBoundary(
      "t1.proctime between t2.proctime - interval '1' hour " +
        "and t2.proctime + interval '1' hour",
      -3600000,
      3600000,
      "proctime")

    verifyTimeBoundary(
      "t1.proctime > t2.proctime - interval '1' second and " +
        "t1.proctime < t2.proctime + interval '1' second",
      -999,
      999,
      "proctime")

    verifyTimeBoundary(
      "t1.c >= t2.c - interval '1' second and " +
        "t1.c <= t2.c + interval '1' second",
      -1000,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c >= t2.c and " +
        "t1.c <= t2.c + interval '1' second",
      0,
      1000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c >= t2.c + interval '1' second and " +
        "t1.c <= t2.c + interval '10' second",
      1000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t2.c - interval '1' second <= t1.c and " +
        "t2.c + interval '10' second >= t1.c",
      -1000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c - interval '2' second >= t2.c + interval '1' second -" +
        "interval '10' second and " +
        "t1.c <= t2.c + interval '10' second",
      -7000,
      10000,
      "rowtime")

    verifyTimeBoundary(
      "t1.c >= t2.c - interval '10' second and " +
        "t1.c <= t2.c - interval '5' second",
      -10000,
      -5000,
      "rowtime")
  }

  @Test
  def testJoinRemainConditionConvert(): Unit = {
    streamUtil.addTable[(Int, Long, Int)]("MyTable3", 'a, 'b.rowtime, 'c, 'proctime.proctime)
    streamUtil.addTable[(Int, Long, Int)]("MyTable4", 'a, 'b.rowtime, 'c, 'proctime.proctime)
    val query =
      "SELECT t1.a, t2.c FROM MyTable3 as t1 join MyTable4 as t2 on t1.a = t2.a and " +
        "t1.b >= t2.b - interval '10' second and t1.b <= t2.b - interval '5' second and " +
        "t1.c > t2.c"
    // The equi-join predicate should also be included
    verifyRemainConditionConvert(
      query,
      "AND(=($0, $4), >($2, $6))")

    val query1 =
      "SELECT t1.a, t2.c FROM MyTable3 as t1 join MyTable4 as t2 on t1.a = t2.a and " +
        "t1.b >= t2.b - interval '10' second and t1.b <= t2.b - interval '5' second "
    verifyRemainConditionConvert(
      query1,
      "=($0, $4)")

    streamUtil.addTable[(Int, Long, Int)]("MyTable5", 'a, 'b, 'c, 'proctime.proctime)
    streamUtil.addTable[(Int, Long, Int)]("MyTable6", 'a, 'b, 'c, 'proctime.proctime)
    val query2 =
      "SELECT t1.a, t2.c FROM MyTable5 as t1 join MyTable6 as t2 on t1.a = t2.a and " +
        "t1.proctime >= t2.proctime - interval '10' second " +
        "and t1.proctime <= t2.proctime - interval '5' second and " +
        "t1.c > t2.c"
    verifyRemainConditionConvert(
      query2,
      "AND(=($0, $4), >($2, $6))")
  }

  private def verifyTimeBoundary(
      timeSql: String,
      expLeftSize: Long,
      expRightSize: Long,
      expTimeType: String): Unit = {
    val query =
      "SELECT t1.a, t2.b FROM MyTable as t1 join MyTable2 as t2 on t1.a = t2.a and " + timeSql

    val resultTable = streamUtil.tableEnv.sqlQuery(query)
    val relNode = resultTable.getRelNode
    val joinNode = relNode.getInput(0).asInstanceOf[LogicalJoin]
    val (windowBounds, _) =
      WindowJoinUtil.extractWindowBoundsFromPredicate(
        joinNode.getCondition,
        4,
        joinNode.getRowType,
        joinNode.getCluster.getRexBuilder,
        streamUtil.tableEnv.getConfig)

    val timeTypeStr =
      if (windowBounds.get.isEventTime) "rowtime"
      else  "proctime"
    assertEquals(expLeftSize, windowBounds.get.leftLowerBound)
    assertEquals(expRightSize, windowBounds.get.leftUpperBound)
    assertEquals(expTimeType, timeTypeStr)
  }

  private def verifyRemainConditionConvert(
      query: String,
      expectCondStr: String): Unit = {

    val resultTable = streamUtil.tableEnv.sqlQuery(query)
    val relNode = resultTable.getRelNode
    val joinNode = relNode.getInput(0).asInstanceOf[LogicalJoin]
    val (_, remainCondition) =
      WindowJoinUtil.extractWindowBoundsFromPredicate(
        joinNode.getCondition,
        4,
        joinNode.getRowType,
        joinNode.getCluster.getRexBuilder,
        streamUtil.tableEnv.getConfig)

    val actual: String = remainCondition.getOrElse("").toString

    assertEquals(expectCondStr, actual)
  }
}
