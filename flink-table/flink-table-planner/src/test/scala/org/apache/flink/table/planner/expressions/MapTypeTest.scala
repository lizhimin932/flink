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
package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.{DataTypes, _}
import org.apache.flink.table.expressions.ApiExpressionUtils.valueLiteral
import org.apache.flink.table.planner.expressions.utils.MapTypeTestBase
import org.apache.flink.table.planner.utils.DateTimeTestUtil.{localDate, localDateTime, localTime => gLocalTime}

import org.junit.Test

import java.time.{LocalDateTime => JLocalTimestamp}

class MapTypeTest extends MapTypeTestBase {

  @Test
  def testInputTypeGeneralization(): Unit = {
    testAllApis(map(1d, "ABC"), "MAP[CAST(1 AS DOUBLE), 'ABC']", "{1.0=ABC}")
  }

  @Test
  def testItem(): Unit = {
    testSqlApi("f0['map is null']", "NULL")
    testSqlApi("f1['map is empty']", "NULL")
    testSqlApi("f2['b']", "13")
    testSqlApi("f3[1]", "NULL")
    testSqlApi("f3[12]", "a")
    testSqlApi("f2[f3[12]]", "12")
  }

  @Test
  def testMapLiteral(): Unit = {
    // primitive literals
    testAllApis(map(1, 1), "MAP[1, 1]", "{1=1}")

    testAllApis(map(true, true), "map[TRUE, TRUE]", "{TRUE=TRUE}")

    // object literals
    testTableApi(map(BigDecimal(1), BigDecimal(1)), "{1=1}")

    testAllApis(map(map(1, 2), map(3, 4)), "MAP[MAP[1, 2], MAP[3, 4]]", "{{1=2}={3=4}}")

    testAllApis(map(1, nullOf(DataTypes.INT)), "map[1, NULLIF(1,1)]", "{1=NULL}")

    // explicit conversion
    testAllApis(map(1, 2L), "MAP[1, CAST(2 AS BIGINT)]", "{1=2}")

    testAllApis(
      map(valueLiteral(localDate("1985-04-11")), valueLiteral(gLocalTime("14:15:16"))),
      "MAP[DATE '1985-04-11', TIME '14:15:16']",
      "{1985-04-11=14:15:16}")

    // There is no timestamp literal function in Java String Table API,
    // toTimestamp is casting string to TIMESTAMP(3) which is not the same to timestamp literal.
    testTableApi(
      map(valueLiteral(gLocalTime("14:15:16")), valueLiteral(localDateTime("1985-04-11 14:15:16"))),
      "{14:15:16=1985-04-11 14:15:16}")
    testSqlApi(
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16']",
      "{14:15:16=1985-04-11 14:15:16}")

    testAllApis(
      map(
        valueLiteral(gLocalTime("14:15:16")),
        valueLiteral(localDateTime("1985-04-11 14:15:16.123"))),
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16.123']",
      "{14:15:16=1985-04-11 14:15:16.123}"
    )

    testTableApi(
      map(
        valueLiteral(gLocalTime("14:15:16")),
        valueLiteral(JLocalTimestamp.of(1985, 4, 11, 14, 15, 16, 123456000))),
      "{14:15:16=1985-04-11 14:15:16.123456}")

    testSqlApi(
      "MAP[TIME '14:15:16', TIMESTAMP '1985-04-11 14:15:16.123456']",
      "{14:15:16=1985-04-11 14:15:16.123456}")

    testAllApis(
      map(BigDecimal(2.0002), BigDecimal(2.0003)),
      "MAP[CAST(2.0002 AS DECIMAL(5, 4)), CAST(2.0003 AS DECIMAL(5, 4))]",
      "{2.0002=2.0003}")

    // implicit type cast only works on SQL API
    testSqlApi("MAP['k1', CAST(1 AS DOUBLE)]", "{k1=1.0}")
  }

  @Test
  def testMapField(): Unit = {
    testAllApis(map('f4, 'f5), "MAP[f4, f5]", "{foo=12}")

    testAllApis(map('f4, 'f1), "MAP[f4, f1]", "{foo={}}")

    testAllApis(map('f2, 'f3), "MAP[f2, f3]", "{{a=12, b=13}={12=a, 13=b}}")

    testAllApis(map('f1.at("a"), 'f5), "MAP[f1['a'], f5]", "{NULL=12}")

    testAllApis('f1, "f1", "{}")

    testAllApis('f2, "f2", "{a=12, b=13}")

    testAllApis('f2.at("a"), "f2['a']", "12")

    testAllApis('f3.at(12), "f3[12]", "a")

    testAllApis(map('f4, 'f3).at("foo").at(13), "MAP[f4, f3]['foo'][13]", "b")
  }

  @Test
  def testMapOperations(): Unit = {

    // comparison
    testAllApis('f1 === 'f2, "f1 = f2", "FALSE")

    testAllApis('f3 === 'f7, "f3 = f7", "TRUE")

    testAllApis('f5 === 'f2.at("a"), "f5 = f2['a']", "TRUE")

    testAllApis('f8 === 'f9, "f8 = f9", "TRUE")

    testAllApis('f10 === 'f11, "f10 = f11", "TRUE")

    testAllApis('f8 !== 'f9, "f8 <> f9", "FALSE")

    testAllApis('f10 !== 'f11, "f10 <> f11", "FALSE")

    testAllApis('f0.at("map is null"), "f0['map is null']", "NULL")

    testAllApis('f1.at("map is empty"), "f1['map is empty']", "NULL")

    testAllApis('f2.at("b"), "f2['b']", "13")

    testAllApis('f3.at(1), "f3[1]", "NULL")

    testAllApis('f3.at(12), "f3[12]", "a")

    testAllApis('f3.cardinality(), "CARDINALITY(f3)", "2")

    testAllApis('f2.at("a").isNotNull, "f2['a'] IS NOT NULL", "TRUE")

    testAllApis('f2.at("a").isNull, "f2['a'] IS NULL", "FALSE")

    testAllApis('f2.at("c").isNotNull, "f2['c'] IS NOT NULL", "FALSE")

    testAllApis('f2.at("c").isNull, "f2['c'] IS NULL", "TRUE")
  }

  @Test
  def testMapTypeCasting(): Unit = {
    testTableApi('f2.cast(DataTypes.MAP(DataTypes.STRING, DataTypes.INT)), "{a=12, b=13}")
  }
}
