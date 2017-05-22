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

package org.apache.flink.cep.scala.examples.events

class TemperatureAlert(rackID: Int = -1) {

  def getRackID: Int = rackID

  override def equals(obj: Any): Boolean = obj match {
    case other: TemperatureAlert => rackID == other.getRackID
    case _ => false
  }

  override def hashCode: Int = rackID

  override def toString: String = "TemperatureAlert(" + getRackID + ")"
}
