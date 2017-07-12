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
package org.apache.flink.table.api.scala.stream.table.validation

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.stream.utils.StreamTestData
import org.apache.flink.table.runtime.datastream.StreamITCase
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.api.TableException
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

class CalcValidationTest extends TableTestBase {

  @Test(expected = classOf[TableException])
  def testAsWithToManyFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testAsWithAmbiguousFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'b)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[TableException])
  def testOnlyFieldRefInAs(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b as 'c, 'd)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
