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

package org.apache.flink.table.codegen.calls

import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.table.codegen.calls.CallGenerator.generateCallIfArgsNotNull
import org.apache.flink.table.codegen.{CodeGenerator, GeneratedExpression}
import org.apache.flink.table.runtime.functions.DateTimeFunctions

class StrToDateCallGen extends CallGenerator {
  override def generate(codeGenerator: CodeGenerator,
                        operands: Seq[GeneratedExpression]): GeneratedExpression = {
    if (operands.last.literal) {
      generateCallIfArgsNotNull(codeGenerator.nullCheck, SqlTimeTypeInfo.DATE, operands) {
        terms => s"""
                    |org.apache.flink.table.runtime.functions.
                    |DateTimeFunctions$$.MODULE$$.strToDate(${terms.head}, ${terms.last})
              """.stripMargin
      }
    } else {
      generateCallIfArgsNotNull(codeGenerator.nullCheck, SqlTimeTypeInfo.TIMESTAMP, operands) {
        terms => s"""
                    |org.apache.flink.table.runtime.functions.
                    |DateTimeFunctions$$.MODULE$$.strToTimestamp(${terms.head}, ${terms.last})
              """.stripMargin
      }
    }
  }
}
