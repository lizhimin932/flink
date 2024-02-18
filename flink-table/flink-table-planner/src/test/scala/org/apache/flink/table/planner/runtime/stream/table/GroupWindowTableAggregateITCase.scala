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
package org.apache.flink.table.planner.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableAggFunctions.Top3
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.TimeTestUtil.TimestampAndWatermarkWithOffset
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.TestTemplate
import org.junit.jupiter.api.extension.ExtendWith

import java.math.BigDecimal
import java.time.Duration

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class GroupWindowTableAggregateITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  val data = List(
    (1L, 1, "Hi"),
    (2L, 2, "Hello"),
    (4L, 2, "Hello"),
    (8L, 3, "Hello world"),
    (16L, 3, "Hello world"))

  val data2 = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world"),
    (32L, 4, 4d, 4f, new BigDecimal("4"), null.asInstanceOf[String])
  )

  @TestTemplate
  def testProcessingTimeSlidingGroupWindowOverCount(): Unit = {
    tEnv.getConfig.setIdleStateRetention(Duration.ofHours(1))
    val stream = failingDataSource(tupleData3)
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'proctime.proctime)

    val top3 = new Top3

    val windowedTable = table
      .window(Slide.over(4.rows).every(2.rows).on('proctime).as('w))
      .groupBy('w, 'long)
      .flatAggregate(call(top3, 'int).as('x, 'y))
      .select('long, 'x, 'y)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "4,8,3",
      "4,9,2",
      "4,10,1",
      "5,12,3",
      "5,13,2",
      "5,14,1",
      "6,17,3",
      "6,18,2",
      "6,19,1",
      "6,19,3",
      "6,20,2",
      "6,21,1")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeSessionGroupWindowOverTime(): Unit = {
    // To verify the "merge" functionality, we create this test with the following characteristics:
    // 1. set the Parallelism to 1, and have the test data out of order
    // 2. create a waterMark with 10ms offset to delay the window emission by 10ms
    val sessionWindowTestData = List(
      (1L, 1, "Hello"),
      (2L, 2, "Hello"),
      (8L, 8, "Hello"),
      (9L, 9, "Hello World"),
      (4L, 4, "Hello"),
      (16L, 16, "Hello"))

    val top3 = new Top3

    val stream = failingDataSource(sessionWindowTestData)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Long, Int, String)](10L))
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'rowtime.rowtime)

    val windowedTable = table
      .window(Session.withGap(5.milli).on('rowtime).as('w))
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("Hello,2,3", "Hello,4,2", "Hello,8,1", "Hello World,9,1", "Hello,16,1")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testAllProcessingTimeTumblingGroupWindowOverCount(): Unit = {
    tEnv.getConfig.setIdleStateRetention(Duration.ofHours(1))
    val stream = failingDataSource(tupleData3)
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'proctime.proctime)
    val top3 = new Top3

    val windowedTable = table
      .window(Tumble.over(7.rows).on('proctime).as('w))
      .groupBy('w)
      .flatAggregate(top3('int))
      .select('f0, 'f1)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("5,3", "6,2", "7,1", "12,3", "13,2", "14,1", "19,3", "20,2", "21,1")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeTumblingWindow(): Unit = {
    val stream = failingDataSource(tupleData3)
      .assignTimestampsAndWatermarks(new TimestampAndWatermarkWithOffset[(Int, Long, String)](0L))
    val table = stream.toTable(tEnv, 'int, 'long, 'string, 'rowtime.rowtime)
    val top3 = new Top3

    val windowedTable = table
      .window(Tumble.over(10.milli).on('rowtime).as('w))
      .groupBy('w, 'long)
      .flatAggregate(top3('int).as('x, 'y))
      .select('w.start, 'w.end, 'long, 'x, 'y + 1)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "1970-01-01T00:00,1970-01-01T00:00:00.010,1,1,2",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,2,2,3",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,2,3,2",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,3,4,4",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,3,5,3",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,3,6,2",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,4,7,4",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,4,8,3",
      "1970-01-01T00:00,1970-01-01T00:00:00.010,4,9,2",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,4,10,2",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,5,13,4",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,5,14,3",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,5,15,2",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,6,17,4",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,6,18,3",
      "1970-01-01T00:00:00.010,1970-01-01T00:00:00.020,6,19,2",
      "1970-01-01T00:00:00.020,1970-01-01T00:00:00.030,6,21,2",
      "1970-01-01T00:00:00.020,1970-01-01T00:00:00.030,6,20,3"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testGroupWindowWithoutKeyInProjection(): Unit = {
    val data = List(
      (1L, 1, "Hi", 1, 1),
      (2L, 2, "Hello", 2, 2),
      (4L, 2, "Hello", 2, 2),
      (8L, 3, "Hello world", 3, 3),
      (16L, 3, "Hello world", 3, 3))

    val stream = failingDataSource(data)
    val table = stream.toTable(tEnv, 'long, 'int, 'string, 'int2, 'int3, 'proctime.proctime)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide.over(2.rows).every(1.rows).on('proctime).as('w))
      .groupBy('w, 'int2, 'int3, 'string)
      .flatAggregate(top3('int))
      .select('f0, 'f1)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq("2,1", "2,2", "3,1", "3,2")
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  // ----------------------------------------------------------------------------------------------
  // Sliding windows
  // ----------------------------------------------------------------------------------------------

  @TestTemplate
  def testAllEventTimeSlidingGroupWindowOverTime(): Unit = {
    // please keep this test in sync with the bounded variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide.over(5.milli).every(2.milli).on('long).as('w))
      .groupBy('w)
      .flatAggregate(top3('int))
      .select('f0, 'f1, 'w.start, 'w.end, 'w.rowtime)

    val sink = new TestingAppendSink
    windowedTable
      .toDataStream(
        DataTypes.ROW(
          DataTypes.INT(),
          DataTypes.INT(),
          DataTypes.TIMESTAMP(3),
          DataTypes.TIMESTAMP(3),
          DataTypes.TIMESTAMP(3)))
      .addSink(sink)
    env.execute()

    val expected = Seq(
      "1,2,1969-12-31T23:59:59.998,1970-01-01T00:00:00.003,1970-01-01T00:00:00.002",
      "2,1,1969-12-31T23:59:59.998,1970-01-01T00:00:00.003,1970-01-01T00:00:00.002",
      "2,3,1970-01-01T00:00,1970-01-01T00:00:00.005,1970-01-01T00:00:00.004",
      "5,1,1970-01-01T00:00,1970-01-01T00:00:00.005,1970-01-01T00:00:00.004",
      "2,2,1970-01-01T00:00,1970-01-01T00:00:00.005,1970-01-01T00:00:00.004",
      "2,3,1970-01-01T00:00:00.002,1970-01-01T00:00:00.007,1970-01-01T00:00:00.006",
      "2,2,1970-01-01T00:00:00.002,1970-01-01T00:00:00.007,1970-01-01T00:00:00.006",
      "5,1,1970-01-01T00:00:00.002,1970-01-01T00:00:00.007,1970-01-01T00:00:00.006",
      "3,3,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009,1970-01-01T00:00:00.008",
      "3,2,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009,1970-01-01T00:00:00.008",
      "5,1,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009,1970-01-01T00:00:00.008",
      "3,2,1970-01-01T00:00:00.006,1970-01-01T00:00:00.011,1970-01-01T00:00:00.010",
      "3,1,1970-01-01T00:00:00.006,1970-01-01T00:00:00.011,1970-01-01T00:00:00.010",
      "3,1,1970-01-01T00:00:00.008,1970-01-01T00:00:00.013,1970-01-01T00:00:00.012",
      "4,1,1970-01-01T00:00:00.012,1970-01-01T00:00:00.017,1970-01-01T00:00:00.016",
      "4,1,1970-01-01T00:00:00.014,1970-01-01T00:00:00.019,1970-01-01T00:00:00.018",
      "4,1,1970-01-01T00:00:00.016,1970-01-01T00:00:00.021,1970-01-01T00:00:00.020",
      "4,1,1970-01-01T00:00:00.028,1970-01-01T00:00:00.033,1970-01-01T00:00:00.032",
      "4,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.035,1970-01-01T00:00:00.034",
      "4,1,1970-01-01T00:00:00.032,1970-01-01T00:00:00.037,1970-01-01T00:00:00.036"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeSlidingGroupWindowOverTimeOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the bounded variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide.over(5.milli).every(4.milli).on('long).as('w))
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "Hello,2,2,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello,5,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hallo,2,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello world,3,1,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009",
      "Hello world,3,1,1970-01-01T00:00:00.008,1970-01-01T00:00:00.013",
      "Hello,3,2,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009",
      "Hi,1,1,1970-01-01T00:00,1970-01-01T00:00:00.005",
      "Hello,5,1,1970-01-01T00:00:00.004,1970-01-01T00:00:00.009",
      "Hello world,4,1,1970-01-01T00:00:00.012,1970-01-01T00:00:00.017",
      "null,4,1,1970-01-01T00:00:00.028,1970-01-01T00:00:00.033",
      "Hello world,4,1,1970-01-01T00:00:00.016,1970-01-01T00:00:00.021",
      "null,4,1,1970-01-01T00:00:00.032,1970-01-01T00:00:00.037"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeSlidingGroupWindowOverTimeNonOverlappingSplitPane(): Unit = {
    // please keep this test in sync with the bounded variant
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
    val table = stream.toTable(tEnv, 'long.rowtime, 'int, 'double, 'float, 'bigdec, 'string)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide.over(3.milli).every(10.milli).on('long).as('w))
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()

    val expected = Seq(
      "null,4,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.033",
      "Hallo,2,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "Hi,1,1,1970-01-01T00:00,1970-01-01T00:00:00.003"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }

  @TestTemplate
  def testEventTimeGroupWindowWithoutExplicitTimeField(): Unit = {
    val stream = failingDataSource(data2)
      .assignTimestampsAndWatermarks(
        new TimestampAndWatermarkWithOffset[(Long, Int, Double, Float, BigDecimal, String)](0L))
      .map(t => (t._2, t._6))
    val table = stream.toTable(tEnv, 'int, 'string, 'rowtime.rowtime)

    val top3 = new Top3
    val windowedTable = table
      .window(Slide.over(3.milli).every(10.milli).on('rowtime).as('w))
      .groupBy('w, 'string)
      .flatAggregate(top3('int))
      .select('string, 'f0, 'f1, 'w.start, 'w.end)

    val sink = new TestingAppendSink
    windowedTable.toDataStream.addSink(sink)
    env.execute()
    val expected = Seq(
      "Hallo,2,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "Hi,1,1,1970-01-01T00:00,1970-01-01T00:00:00.003",
      "null,4,1,1970-01-01T00:00:00.030,1970-01-01T00:00:00.033"
    )
    assertThat(sink.getAppendResults.sorted).isEqualTo(expected.sorted)
  }
}
