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

package org.apache.flink.table.api.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{ResultKind, TableException}
import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.runtime.stream.sql.FunctionITCase.{SimpleScalarFunction, TestUDF}
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._

import org.hamcrest.Matchers.containsString
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}
import org.junit.Test

import scala.collection.JavaConverters._

class BatchTableEnvironmentTest extends TableTestBase {

  @Test
  def testSqlWithoutRegistering(): Unit = {
    val util = batchTestUtil()
    val table = util.addTable[(Long, Int, String)]("tableName", 'a, 'b, 'c)

    val sqlTable = util.tableEnv.sqlQuery(s"SELECT a, b, c FROM $table WHERE b > 12")

    val expected = unaryNode(
      "DataSetCalc",
      batchTableNode(table),
      term("select", "a, b, c"),
      term("where", ">(b, 12)"))

    util.verifyTable(sqlTable, expected)

    val table2 = util.addTable[(Long, Int, String)]('d, 'e, 'f)

    val sqlTable2 = util.tableEnv.sqlQuery(s"SELECT d, e, f FROM $table, $table2 WHERE c = d")

    val join = unaryNode(
      "DataSetJoin",
      binaryNode(
        "DataSetCalc",
        batchTableNode(table),
        batchTableNode(table2),
        term("select", "c")),
      term("where", "=(c, d)"),
      term("join", "c, d, e, f"),
      term("joinType", "InnerJoin"))

    val expected2 = unaryNode(
      "DataSetCalc",
      join,
      term("select", "d, e, f"))

    util.verifyTable(sqlTable2, expected2)
  }


  @Test
  def testExecuteSqlWithCreateDropTable(): Unit = {
    val util = batchTestUtil()

    val createTableStmt =
      """
        |CREATE TABLE tbl1 (
        |  a bigint,
        |  b int,
        |  c varchar
        |) with (
        |  'connector' = 'COLLECTION',
        |  'is-bounded' = 'true'
        |)
      """.stripMargin
    val tableResult1 = util.tableEnv.executeSql(createTableStmt)
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")))

    val tableResult2 = util.tableEnv.executeSql("ALTER TABLE tbl1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("connector" -> "COLLECTION", "is-bounded" -> "true", "k1" -> "a", "k2" -> "b").asJava,
      util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
        .getTable(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")).getProperties)

    val tableResult3 = util.tableEnv.executeSql("DROP TABLE tbl1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .tableExists(ObjectPath.fromString(s"${util.tableEnv.getCurrentDatabase}.tbl1")))
  }

  @Test
  def testExecuteSqlWithCreateAlterDropDatabase(): Unit = {
    val util = batchTestUtil()
    val tableResult1 = util.tableEnv.executeSql("CREATE DATABASE db1 COMMENT 'db1_comment'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .databaseExists("db1"))

    val tableResult2 = util.tableEnv.executeSql("ALTER DATABASE db1 SET ('k1' = 'a', 'k2' = 'b')")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertEquals(
      Map("k1" -> "a", "k2" -> "b").asJava,
      util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
        .getDatabase("db1").getProperties)

    val tableResult3 = util.tableEnv.executeSql("DROP DATABASE db1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .databaseExists("db1"))
  }

  @Test
  def testExecuteSqlWithCreateDropFunction(): Unit = {
    val util = batchTestUtil()
    val funcName = classOf[TestUDF].getName
    val funcName2 = classOf[SimpleScalarFunction].getName

    val tableResult1 = util.tableEnv.executeSql(
      s"CREATE FUNCTION default_database.f1 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult1.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult2 = util.tableEnv.executeSql(
      s"ALTER FUNCTION default_database.f1 AS '$funcName2'")
    assertEquals(ResultKind.SUCCESS, tableResult2.getResultKind)
    assertTrue(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult3 = util.tableEnv.executeSql("DROP FUNCTION default_database.f1")
    assertEquals(ResultKind.SUCCESS, tableResult3.getResultKind)
    assertFalse(util.tableEnv.getCatalog(util.tableEnv.getCurrentCatalog).get()
      .functionExists(ObjectPath.fromString("default_database.f1")))

    val tableResult4 = util.tableEnv.executeSql(
      s"CREATE TEMPORARY SYSTEM FUNCTION default_database.f2 AS '$funcName'")
    assertEquals(ResultKind.SUCCESS, tableResult4.getResultKind)
    assertTrue(util.tableEnv.listUserDefinedFunctions().contains("f2"))

    val tableResult5 = util.tableEnv.executeSql(
      "DROP TEMPORARY SYSTEM FUNCTION default_database.f2")
    assertEquals(ResultKind.SUCCESS, tableResult5.getResultKind)
    assertFalse(util.tableEnv.listUserDefinedFunctions().contains("f2"))
  }

  @Test
  def testExecuteSqlWithUnsupportedStmt(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Long, Int, String)]("MyTable", 'a, 'b, 'c)

    thrown.expect(classOf[TableException])
    thrown.expectMessage(containsString("Unsupported SQL query!"))
    // TODO supports select later
    util.tableEnv.executeSql("select * from MyTable")
  }
}
