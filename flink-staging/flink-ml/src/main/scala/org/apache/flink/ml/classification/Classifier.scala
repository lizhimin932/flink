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

package org.apache.flink.ml.classification

import org.apache.flink.api.scala._
import org.apache.flink.ml.evaluation.ClassificationScores
import org.apache.flink.ml.pipeline.Predictor

/** Trait that classification algorithms should implement
  *
  * @tparam Self Type of the implementing class
  */
trait Classifier[Self] extends Predictor[Self]{
  that: Self =>

  /** Calculates the performance score for the algorithm, given a DataSet of (truth, prediction)
    * tuples
    *
    * @param input A DataSet of (truth, prediction) tuples
    * @return A DataSet containing one Double that indicates the score of the predictor
    */
  override def calculateScore(input: DataSet[(Double, Double)]): DataSet[Double] = {
    ClassificationScores.accuracyScore.evaluate(input)
  }
}
