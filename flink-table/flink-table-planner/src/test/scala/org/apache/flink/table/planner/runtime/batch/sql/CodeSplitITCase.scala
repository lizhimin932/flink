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

package org.apache.flink.table.planner.runtime.batch.sql

import org.apache.flink.core.testutils.FlinkMatchers
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData.{nullablesOfData3, smallData3, type3}
import org.apache.flink.types.Row

import org.hamcrest.MatcherAssert
import org.junit.{Assert, Before, Test}

import scala.collection.Seq

class CodeSplitITCase extends BatchTestBase {

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection("SmallTable3", smallData3, type3, "a, b, c", nullablesOfData3)
  }

  @Test
  def testSelectManyColumns(): Unit = {
    val sql = new StringBuilder("SELECT ")
    for (i <- 1 to 1000) {
      sql.append(s"a + $i * b, ")
    }
    sql.append("a, b FROM SmallTable3")

    val results = new scala.collection.mutable.ArrayBuffer[Row]()
    for ((a, b) <- Seq((1, 1), (2, 2), (3, 2))) {
      val r = new Row(1002)
      for (i <- 1 to 1000) {
        r.setField(i - 1, a + i * b)
      }
      r.setField(1000, a)
      r.setField(1001, b)
      results.append(r)
    }

    runTest(sql.mkString, results)
  }

  @Test
  def testManyOrsInCondition(): Unit = {
    val sql = new StringBuilder("SELECT a, b FROM SmallTable3 WHERE ")
    for (i <- 1 to 300) {
      sql.append(s"(a + b > $i AND a * b > $i) OR ")
    }
    sql.append("CAST((a + b > 1 AND a * b > 1) AS VARCHAR) = 'true'")

    runTest(sql.mkString, Seq(row(2, 2), row(3, 2)))
  }

  @Test
  def testManyAggregations(): Unit = {
    val sql = new StringBuilder("SELECT ")
    for (i <- 1 to 300) {
      sql.append(s"SUM(a + $i * b)")
      if (i != 300) {
        sql.append(", ")
      }
    }
    sql.append(" FROM SmallTable3")

    val result = new Row(300)
    for (i <- 1 to 300) {
      result.setField(i - 1, 6 + 5 * i)
    }

    runTest(sql.mkString, Seq(result))
  }

  private[flink] def runTest(sql: String, results: Seq[Row]): Unit = {
    tEnv.getConfig.getConfiguration.setInteger(
      TableConfigOptions.MAX_LENGTH_GENERATED_CODE, 4000)
    tEnv.getConfig.getConfiguration.setInteger(
      TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, 10000)
    checkResult(sql.mkString, results)

    tEnv.getConfig.getConfiguration.setInteger(
      TableConfigOptions.MAX_LENGTH_GENERATED_CODE, Int.MaxValue)
    tEnv.getConfig.getConfiguration.setInteger(
      TableConfigOptions.MAX_MEMBERS_GENERATED_CODE, Int.MaxValue)
    try {
      checkResult(sql, results)
      Assert.fail("Expecting compiler exception")
    } catch {
      case e: Exception =>
        MatcherAssert.assertThat(e, FlinkMatchers.containsMessage("grows beyond 64 KB"))
    }
  }
}
