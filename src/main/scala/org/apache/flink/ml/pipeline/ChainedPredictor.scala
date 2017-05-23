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

package org.apache.flink.ml.pipeline

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.DataSet
import org.apache.flink.ml.common.ParameterMap

/** [[Predictor]] which represents a pipeline of possibly multiple [[Transformer]] and a trailing
  * [[Predictor]].
  *
  * The [[ChainedPredictor]] can be used as a regular [[Predictor]]. Upon calling the fit method,
  * the input data is piped through all preceding [[Transformer]] in the pipeline and the resulting
  * data is given to the trailing [[Predictor]]. The same holds true for the predict operation.
  *
  * The pipeline mechanism has been inspired by scikit-learn
  *
  * @param transformer Preceding [[Transformer]] of the pipeline
  * @param predictor Trailing [[Predictor]] of the pipeline
  * @tparam T Type of the preceding [[Transformer]]
  * @tparam P Type of the trailing [[Predictor]]
  */
case class ChainedPredictor[T <: Transformer[T], P <: Predictor[P]](transformer: T, predictor: P)
  extends Predictor[ChainedPredictor[T, P]]{}

object ChainedPredictor{

  /** [[PredictDataSetOperation]] for the [[ChainedPredictor]].
    *
    * The [[PredictDataSetOperation]] requires the [[TransformDataSetOperation]] of the preceding
    * [[Transformer]] and the [[PredictDataSetOperation]] of the trailing [[Predictor]]. Upon
    * calling predict, the testing data is first transformed by the preceding [[Transformer]] and
    * the result is then used to calculate the prediction via the trailing [[Predictor]].
    *
    * @param transformOperation [[TransformDataSetOperation]] for the preceding [[Transformer]]
    * @param predictOperation [[PredictDataSetOperation]] for the trailing [[Predictor]]
    * @tparam T Type of the preceding [[Transformer]]
    * @tparam P Type of the trailing [[Predictor]]
    * @tparam Testing Type of the testing data
    * @tparam Intermediate Type of the intermediate data produced by the preceding [[Transformer]]
    * @tparam Prediction Type of the predicted data generated by the trailing [[Predictor]]
    * @return
    */
  implicit def chainedPredictOperation[
      T <: Transformer[T],
      P <: Predictor[P],
      Testing,
      Intermediate,
      Prediction](
      implicit transformOperation: TransformDataSetOperation[T, Testing, Intermediate],
      predictOperation: PredictDataSetOperation[P, Intermediate, Prediction])
    : PredictDataSetOperation[ChainedPredictor[T, P], Testing, Prediction] = {

    new PredictDataSetOperation[ChainedPredictor[T, P], Testing, Prediction] {
      override def predictDataSet(
          instance: ChainedPredictor[T, P],
          predictParameters: ParameterMap,
          input: DataSet[Testing])
        : DataSet[Prediction] = {

        val testing = instance.transformer.transform(input, predictParameters)
        instance.predictor.predict(testing, predictParameters)
      }
    }
  }

  /** [[FitOperation]] for the [[ChainedPredictor]].
    *
    * The [[FitOperation]] requires the [[FitOperation]] and the [[TransformDataSetOperation]] of
    * the preceding [[Transformer]] as well as the [[FitOperation]] of the trailing [[Predictor]].
    * Upon calling fit, the preceding [[Transformer]] is first fitted to the training data.
    * The training data is then transformed by the fitted [[Transformer]]. The transformed data
    * is then used to fit the [[Predictor]].
    *
    * @param fitOperation [[FitOperation]] of the preceding [[Transformer]]
    * @param transformOperation [[TransformDataSetOperation]] of the preceding [[Transformer]]
    * @param predictorFitOperation [[PredictDataSetOperation]] of the trailing [[Predictor]]
    * @tparam L Type of the preceding [[Transformer]]
    * @tparam R Type of the trailing [[Predictor]]
    * @tparam I Type of the training data
    * @tparam T Type of the intermediate data
    * @return
    */
  implicit def chainedFitOperation[L <: Transformer[L], R <: Predictor[R], I, T](implicit
    fitOperation: FitOperation[L, I],
    transformOperation: TransformDataSetOperation[L, I, T],
    predictorFitOperation: FitOperation[R, T]): FitOperation[ChainedPredictor[L, R], I] = {
    new FitOperation[ChainedPredictor[L, R], I] {
      override def fit(
          instance: ChainedPredictor[L, R],
          fitParameters: ParameterMap,
          input: DataSet[I])
        : Unit = {
        instance.transformer.fit(input, fitParameters)
        val intermediateResult = instance.transformer.transform(input, fitParameters)
        instance.predictor.fit(intermediateResult, fitParameters)
      }
    }
  }

  implicit def chainedEvaluationOperation[
      T <: Transformer[T],
      P <: Predictor[P],
      Testing,
      Intermediate,
      PredictionValue](
      implicit transformOperation: TransformDataSetOperation[T, Testing, Intermediate],
      evaluateOperation: EvaluateDataSetOperation[P, Intermediate, PredictionValue],
      testingTypeInformation: TypeInformation[Testing],
      predictionValueTypeInformation: TypeInformation[PredictionValue])
    : EvaluateDataSetOperation[ChainedPredictor[T, P], Testing, PredictionValue] = {
    new EvaluateDataSetOperation[ChainedPredictor[T, P], Testing, PredictionValue] {
      override def evaluateDataSet(
          instance: ChainedPredictor[T, P],
          evaluateParameters: ParameterMap,
          testing: DataSet[Testing])
        : DataSet[(PredictionValue, PredictionValue)] = {
        val intermediate = instance.transformer.transform(testing, evaluateParameters)
        instance.predictor.evaluate(intermediate, evaluateParameters)
      }
    }
  }
}
