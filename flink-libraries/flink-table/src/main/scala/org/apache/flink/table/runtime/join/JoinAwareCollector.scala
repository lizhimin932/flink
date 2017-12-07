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

package org.apache.flink.table.runtime.join

import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.types.Row
import org.apache.flink.util.Collector

/**
  * Collector to track whether there's a joined result.
  */
class JoinAwareCollector extends Collector[Row]{

  private var emitted = false
  private var emittedThisTurn = false
  private var innerCollector: Collector[CRow] = _
  private val cRow: CRow = new CRow()

  def setCollector(collector: Collector[CRow]): Unit = {
    this.innerCollector = collector
  }

  def reset(): Unit = {
    emitted = false
    emittedThisTurn = false
  }

  def resetThisTurn(): Unit = {
    emittedThisTurn = false
  }

  def everEmitted: Boolean = emitted

  def everEmittedThisTurn: Boolean = emittedThisTurn

  /** Collects a record without notifying the collector. **/
  def collectWithoutNotifying(record: Row): Unit = {
    cRow.row = record
    innerCollector.collect(cRow)
  }

  override def collect(record: Row): Unit = {
    emittedThisTurn = true
    emitted = true
    cRow.row = record
    innerCollector.collect(cRow)
  }

  override def close(): Unit = {
    innerCollector.close()
  }
}
