package org.apache.flink.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.utils.TableTestBase
import org.apache.flink.table.utils.TableTestUtil._
import org.junit.Test

class GroupingSetsPlansTest extends TableTestBase {

  @Test
  def testGroupingSetsPlan(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g FROM MyTable " +
                   "GROUP BY GROUPING SETS (b, c)"

    val group1 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b"),
      term("select", "b",
           "AVG(a) AS c")
    )

    val group2 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "c"),
      term("select", "c AS b",
           "AVG(a) AS c")
    )

    val union = binaryNode(
      "DataSetUnion",
      group1, group2,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      union,
      term("select",
           "CASE(i$b, null, b) AS b",
           "CASE(i$c, null, c) AS c",
           "a",
           "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g") // GROUP_ID()
    )

    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testCubePlan(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
                   "GROUPING(b) as gb, GROUPING(c) as gc, " +
                   "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
                   "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
                   "GROUP BY CUBE (b, c)"

    val group1 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b, c"),
      term("select", "b", "c",
           "AVG(a) AS i$b")
    )

    val group2 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b"),
      term("select", "b",
           "AVG(a) AS c")
    )

    val group3 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "c"),
      term("select", "c AS b",
           "AVG(a) AS c")
    )

    val group4 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("select",
           "AVG(a) AS b")
    )

    val union1 = binaryNode(
      "DataSetUnion",
      group1, group2,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union2 = binaryNode(
      "DataSetUnion",
      union1, group3,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union3 = binaryNode(
      "DataSetUnion",
      union2, group4,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      union3,
      term("select",
           "CASE(i$b, null, b) AS b",
           "CASE(i$c, null, c) AS c",
           "a",
           "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g", // GROUP_ID()
           "CASE(i$b, 1, 0) AS gb", // GROUPING(b)
           "CASE(i$c, 1, 0) AS gc", // GROUPING(c)
           "CASE(i$b, 1, 0) AS gib", // GROUPING_ID(b)
           "CASE(i$c, 1, 0) AS gic", // GROUPING_ID(c)
           "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS gid") // GROUPING_ID(b, c)
    )

    util.verifySql(sqlQuery, aggregate)
  }

  @Test
  def testRollupPlan(): Unit = {
    val util = batchTestUtil()
    util.addTable[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val sqlQuery = "SELECT b, c, avg(a) as a, GROUP_ID() as g, " +
                   "GROUPING(b) as gb, GROUPING(c) as gc, " +
                   "GROUPING_ID(b) as gib, GROUPING_ID(c) as gic, " +
                   "GROUPING_ID(b, c) as gid " + " FROM MyTable " +
                   "GROUP BY ROLLUP (b, c)"

    val group1 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b, c"),
      term("select", "b", "c",
           "AVG(a) AS i$b")
    )

    val group2 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("groupBy", "b"),
      term("select", "b",
           "AVG(a) AS c")
    )

    val group3 = unaryNode(
      "DataSetAggregate",
      batchTableNode(0),
      term("select",
           "AVG(a) AS b")
    )

    val union1 = binaryNode(
      "DataSetUnion",
      group1, group2,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val union2 = binaryNode(
      "DataSetUnion",
      union1, group3,
      term("union", "b", "c", "i$b", "i$c", "a")
    )

    val aggregate = unaryNode(
      "DataSetCalc",
      union2,
      term("select",
           "CASE(i$b, null, b) AS b",
           "CASE(i$c, null, c) AS c",
           "a",
           "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS g", // GROUP_ID()
           "CASE(i$b, 1, 0) AS gb", // GROUPING(b)
           "CASE(i$c, 1, 0) AS gc", // GROUPING(c)
           "CASE(i$b, 1, 0) AS gib", // GROUPING_ID(b)
           "CASE(i$c, 1, 0) AS gic", // GROUPING_ID(c)
           "+(*(CASE(i$b, 1, 0), 2), CASE(i$c, 1, 0)) AS gid") // GROUPING_ID(b, c)
    )

    util.verifySql(sqlQuery, aggregate)
  }
}
