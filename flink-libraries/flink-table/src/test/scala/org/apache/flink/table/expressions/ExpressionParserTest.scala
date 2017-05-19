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
package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.Test

/**
  * Functionality test for [[ExpressionParser]]
  */
class ExpressionParserTest extends ExpressionTestBase{

  @Test
  def testBoolLiteral(): Unit = {
    testAllApis(
      Literal(true),
      "true",
      "true",
      "true")

    testAllApis(
      Literal(true),
      "True",
      "tRue",
      "true")

    testAllApis(
      Literal(true),
      "TrUe",
      "tRuE",
      "true")
  }

  override def testData: Any = new Row(0)

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo().asInstanceOf[TypeInformation[Any]]
}
