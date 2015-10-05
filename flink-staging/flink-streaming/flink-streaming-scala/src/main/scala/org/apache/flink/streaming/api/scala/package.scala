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

package org.apache.flink.streaming.api

import _root_.scala.reflect.ClassTag
import language.experimental.macros
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{createTuple2TypeInformation => apiTupleCreator}
import org.apache.flink.api.scala.typeutils.{CaseClassTypeInfo, TypeUtils}
import org.apache.flink.streaming.api.datastream.{ DataStream => JavaStream }
import org.apache.flink.streaming.api.datastream.{ WindowedDataStream => JavaWStream }
import org.apache.flink.streaming.api.datastream.{ SplitStream => SplitJavaStream }
import org.apache.flink.streaming.api.datastream.{ ConnectedStreams => ConnectedJavaStreams }
import org.apache.flink.streaming.api.datastream.{ KeyedStream => KeyedJavaStream }
import language.implicitConversions

package object scala {
  // We have this here so that we always have generated TypeInformationS when
  // using the Scala API
  implicit def createTypeInformation[T]: TypeInformation[T] = macro TypeUtils.createTypeInfo[T]

  implicit def javaToScalaStream[R](javaStream: JavaStream[R]): DataStream[R] =
    new DataStream[R](javaStream)
    
  implicit def javaToScalaGroupedStream[R, K](javaStream: KeyedJavaStream[R, K]):
  KeyedStream[R, K] = new KeyedStream[R, K](javaStream)

  implicit def javaToScalaWindowedStream[R](javaWStream: JavaWStream[R]): WindowedDataStream[R] =
    new WindowedDataStream[R](javaWStream)

  implicit def javaToScalaSplitStream[R](javaStream: SplitJavaStream[R]): SplitStream[R] =
    new SplitStream[R](javaStream)

  implicit def javaToScalaConnectedStream[IN1, IN2](javaStream: ConnectedJavaStreams[IN1, IN2]):
  ConnectedStreams[IN1, IN2] = new ConnectedStreams[IN1, IN2](javaStream)

  implicit def seqToFlinkSource[T: ClassTag: TypeInformation](scalaSeq: Seq[T]) : DataStream[T] =
    StreamExecutionEnvironment.getExecutionEnvironment.fromCollection(scalaSeq)

  private[flink] def fieldNames2Indices(
      typeInfo: TypeInformation[_],
      fields: Array[String]): Array[Int] = {
    typeInfo match {
      case ti: CaseClassTypeInfo[_] =>
        val result = ti.getFieldIndices(fields)

        if (result.contains(-1)) {
          throw new IllegalArgumentException("Fields '" + fields.mkString(", ") +
            "' are not valid for '" + ti.toString + "'.")
        }

        result

      case _ =>
        throw new UnsupportedOperationException("Specifying fields by name is only" +
          "supported on Case Classes (for now).")
    }
  }

  def createTuple2TypeInformation[T1, T2](
      t1: TypeInformation[T1],
      t2: TypeInformation[T2])
    : TypeInformation[(T1, T2)] =
    apiTupleCreator[T1, T2](t1, t2)
}
