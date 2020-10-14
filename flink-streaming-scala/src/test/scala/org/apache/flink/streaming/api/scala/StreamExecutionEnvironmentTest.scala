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

package org.apache.flink.streaming.api.scala

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.mocks.MockSource
import org.apache.flink.api.java.typeutils.GenericTypeInfo
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.junit.Assert.{assertEquals, fail}
import org.junit.Test

import java.util

import scala.collection.JavaConversions._

/**
 * Tests for the [[StreamExecutionEnvironment]].
 */
class StreamExecutionEnvironmentTest {

  /**
   * Verifies that calls to timeWindow() instantiate a regular
   * windowOperator instead of an aligned one.
   */
  @Test
  def testAlignedWindowDeprecation(): Unit = {
    implicit val typeInfo: TypeInformation[Integer] = new MockTypeInfo()
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val stream = env.fromSource(
      new MockSource(Boundedness.CONTINUOUS_UNBOUNDED, 1),
      WatermarkStrategy.noWatermarks(),
      "test source")

    assertEquals(typeInfo, stream.dataType)
  }

  /**
   * Verifies that calls to fromSequence() instantiate a new DataStream
   * that contains a sequence of numbers.
   */
  @Test
  def testFromSequence(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val from = 284
    val to = 618
    env.fromSequence(from, to).addSink(new RichSinkFunction[Long]() {
      val sequence = new util.ArrayList[Long]()

      override def invoke(in: Long): Unit = {
        sequence.add(in)
      }

      override def close(): Unit = {
        if (sequence.size() != to - from + 1) {
          fail(s"Expected: A sequence [$from, $to], but found: sequence " +
            s"(size ${sequence.size()}) : $sequence")
        }
        for (next <- sequence) {
          if (next < from || next > to) {
            fail(s"Expected: A sequence [$from, $to], but found: sequence " +
              s"(size ${sequence.size()}) : $sequence")
          }
        }
      }
    }).setParallelism(1)
    env.execute("from sequence test")
  }

  // --------------------------------------------------------------------------
  //  mocks
  // --------------------------------------------------------------------------

  private class MockTypeInfo extends GenericTypeInfo[Integer](classOf[Integer]) {}
}
