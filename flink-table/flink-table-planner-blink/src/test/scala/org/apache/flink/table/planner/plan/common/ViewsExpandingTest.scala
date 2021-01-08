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

package org.apache.flink.table.planner.plan.common

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.{CatalogBaseTable, CatalogView, CatalogViewImpl, ObjectIdentifier, ObjectPath}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.common.ViewsExpandingTest.PrimitiveScalarFunction
import org.apache.flink.table.planner.utils.{TableFunc0, TableTestBase, TableTestUtil, TableTestUtilBase}

import org.hamcrest.CoreMatchers.is
import org.hamcrest.MatcherAssert.assertThat
import org.junit.jupiter.api.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

import java.util

@RunWith(classOf[Parameterized])
class ViewsExpandingTest(tableTestUtil: TableTestBase => TableTestUtil) extends TableTestBase {

  @Test
  def testMixedSqlTableViewExpanding(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableUtil.addDataStream[(Int, String, Int)]("t1", 'a, 'b, 'c)
    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view1"),
      createSqlView("t1"),
      false)
    tableEnv.createTemporaryView("view2", tableEnv.from("view1"))
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view3"),
      createSqlView("view2"),
      false)
    tableEnv.createTemporaryView("view4", tableEnv.from("view3"))

    tableUtil.verifyExecPlan("select * from view4")
  }

  @Test
  def testTableApiExpanding(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableUtil.addDataStream[(Int, String, Int)]("t1", 'a, 'b, 'c)
    tableEnv.createTemporaryView("view1", tableEnv.from("t1"))
    tableEnv.createTemporaryView("view2", tableEnv.from("view1"))
    tableEnv.createTemporaryView("view3", tableEnv.from("view2"))

    val query = tableEnv.from("view3")
    tableUtil.verifyExecPlan(query)
  }

  @Test
  def testSqlExpanding(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableUtil.addDataStream[(Int, String, Int)]("t1", 'a, 'b, 'c)
    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view1"),
      createSqlView("t1"),
      false)
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view2"),
      createSqlView("view1"),
      false)
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view3"),
      createSqlView("view2"),
      false)

    val query = "SELECT * FROM view3"
    tableUtil.verifyExecPlan(query)
  }

  @Test
  def testViewExpandingWithMismatchRowType(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    val originTableName = "t1"
    tableUtil.addDataStream[(Int, String, Int)](originTableName, 'a, 'b, 'c)
    val aggSqlView = new CatalogViewImpl(
      s"select a, b, count(c) from $originTableName group by a, b",
      s"select a, b, count(c) from $originTableName group by a, b",
      TableSchema.builder()
        .field("a", DataTypes.INT().notNull()) // Change the nullability intentionally.
        .field("b", DataTypes.STRING())
        .field("c", DataTypes.INT())
        .build(),
      new util.HashMap[String, String](),
      ""
    )
    val catalog = tableEnv.getCatalog(tableEnv.getCurrentCatalog).get()
    catalog.createTable(
      new ObjectPath(tableEnv.getCurrentDatabase, "view1"),
      aggSqlView,
      false)
    tableUtil.verifyExecPlan("select * from view1")
  }

  @Test
  def testViewExpandingWithLateralTableFunction(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableEnv.createTemporarySystemFunction("myFunc", new TableFunc0())
    tableEnv.createTemporaryView("source",
      tableEnv.fromValues("danny#21", "julian#55", "fabian#30").as("f0"))
    val createView =
      """
        |CREATE VIEW tmp_view AS
        |  SELECT f0, f1
        |  FROM source as S, LATERAL TABLE(myFunc(f0)) as T(f1, f2)
        |""".stripMargin
    tableEnv.executeSql(createView)
    tableUtil.verifyExecPlan("select * from tmp_view")
  }

  @Test
  def testViewExpandingWithBuiltinFunction(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    val createView =
      """
        |CREATE VIEW tmp_view AS
        |  SELECT CONCAT('a', 'bc', 'def')
        |""".stripMargin
    tableEnv.executeSql(createView)
    val objectID = ObjectIdentifier.of(tableEnv.getCurrentCatalog,
      tableEnv.getCurrentDatabase, "tmp_view")
    val view: CatalogBaseTable = tableEnv.getCatalog(objectID.getCatalogName)
      .get().getTable(objectID.toObjectPath)
    assertThat(view.asInstanceOf[CatalogView].getExpandedQuery,
      is("SELECT `CONCAT`('a', 'bc', 'def')"))
  }

  @Test
  def testViewExpandingWithUDF(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableEnv.createTemporaryFunction("func", classOf[PrimitiveScalarFunction])
    val createView =
      """
        |CREATE VIEW tmp_view AS
        |  SELECT func(1, 2, 'abc')
        |""".stripMargin
    tableEnv.executeSql(createView)
    val objectID = ObjectIdentifier.of(tableEnv.getCurrentCatalog,
      tableEnv.getCurrentDatabase, "tmp_view")
    val view: CatalogBaseTable = tableEnv.getCatalog(objectID.getCatalogName)
      .get().getTable(objectID.toObjectPath)
    assertThat(view.asInstanceOf[CatalogView].getExpandedQuery,
      is("SELECT `default_catalog`.`default_database`.`func`(1, 2, 'abc')"))
  }

  @Test
  def testExpandQueryWithSystemAlias(): Unit = {
    val tableUtil = tableTestUtil(this)
    val tableEnv = tableUtil.tableEnv
    tableEnv.createTemporaryView("source",
      tableEnv.fromValues("danny#21", "julian#55", "fabian#30").as("f0"))
    val createView =
      """
        |create view tmp_view as
        |select * from (
        |  select f0,
        |  row_number() over (partition by f0 order by f0 desc) as rowNum
        |  from source)
        |  where rowNum = 1
        |""".stripMargin
    tableEnv.executeSql(createView)
    val objectID = ObjectIdentifier.of(tableEnv.getCurrentCatalog,
      tableEnv.getCurrentDatabase, "tmp_view")
    val view: CatalogBaseTable = tableEnv.getCatalog(objectID.getCatalogName)
      .get().getTable(objectID.toObjectPath)
    assertThat(view.asInstanceOf[CatalogView].getExpandedQuery,
      is("SELECT *\n"
        + "FROM (SELECT `source`.`f0`, "
        + "ROW_NUMBER() "
        + "OVER (PARTITION BY `source`.`f0` ORDER BY `source`.`f0` DESC) AS `rowNum`\n"
        + "FROM `default_catalog`.`default_database`.`source`)\n"
        + "WHERE `rowNum` = 1"))
  }

  private def createSqlView(originTable: String): CatalogView = {
      new CatalogViewImpl(
        s"select * as c from $originTable",
        s"select * from $originTable",
        TableSchema.builder()
          .field("a", DataTypes.INT())
          .field("b", DataTypes.STRING())
          .field("c", DataTypes.INT())
          .build(),
        new util.HashMap[String, String](),
        ""
      )
  }

}

object ViewsExpandingTest {
  @Parameters
  def parameters(): Array[TableTestBase => TableTestUtilBase] = {
    Array(
      _.batchTestUtil(),
      _.streamTestUtil())
  }

  // --------------------------------------------------------------------------------------------
  // Test functions
  // --------------------------------------------------------------------------------------------
  /**
   * Function that takes and returns primitives.
   */
  class PrimitiveScalarFunction extends ScalarFunction {
    def eval(i: Int, l: Long, s: String): Long = i + l + s.length
  }
}
