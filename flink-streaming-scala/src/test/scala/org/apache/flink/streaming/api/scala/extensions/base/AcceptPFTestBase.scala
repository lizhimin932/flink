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
package org.apache.flink.streaming.api.scala.extensions.base

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.extensions.data.KeyValuePair
import org.apache.flink.util.TestLogger
import org.scalatest.junit.JUnitSuiteLike

/**
  * Common facilities to test the `acceptPartialFunctions` extension
  */
private[extensions] abstract class AcceptPFTestBase extends TestLogger with JUnitSuiteLike {

  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  protected val tuples = env.fromElements(1 -> "hello", 2 -> "world")
  protected val caseObjects = env.fromElements(KeyValuePair(1, "hello"), KeyValuePair(2, "world"))

  protected val keyedTuples = tuples.keyBy(_._1)
  protected val keyedCaseObjects = caseObjects.keyBy(_.id)

}
