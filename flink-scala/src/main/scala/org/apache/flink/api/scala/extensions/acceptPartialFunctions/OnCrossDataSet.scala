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
package org.apache.flink.api.scala.extensions.acceptPartialFunctions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, CrossDataSet}

import scala.reflect.ClassTag

class OnCrossDataSet[L: TypeInformation, R: TypeInformation](ds: CrossDataSet[L, R]) {

  /**
    * Starting from a cross data set, uses the function `fun` to project elements from
    * both the input data sets in the resulting data set
    *
    * @param fun The function that defines the projection of the join
    * @tparam O The return type of the projection, for which type information must be known
    * @return A data set of Os
    */
  def projecting[O: TypeInformation: ClassTag](fun: (L, R) => O): DataSet[O] =
    ds(fun)

}
