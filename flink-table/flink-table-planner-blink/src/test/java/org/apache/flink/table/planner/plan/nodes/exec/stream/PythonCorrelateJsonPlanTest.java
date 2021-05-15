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

package org.apache.flink.table.planner.plan.nodes.exec.stream;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.PythonScalarFunction;
import org.apache.flink.table.planner.utils.MockPythonTableFunction;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization/deserialization for correlate. */
public class PythonCorrelateJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        TableConfig tableConfig = TableConfig.getDefault();
        util = streamTestUtil(tableConfig);
        util.addFunction(
                "TableFunc", new MockPythonTableFunction(), new RowTypeInfo(Types.INT, Types.INT));
        util.addFunction("pyFunc", new PythonScalarFunction("pyFunc"));
        tEnv = util.getTableEnv();

        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  a int,\n"
                        + "  b int,\n"
                        + "  c int,\n"
                        + "  d timestamp(3)\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
    }

    @Test
    public void testPythonTableFunction() {
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a int,\n"
                        + "  b int\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        String sqlQuery =
                "INSERT INTO MySink SELECT x, y FROM MyTable "
                        + "LEFT JOIN LATERAL TABLE(TableFunc(a * a, pyFunc(a, b))) AS T(x, y) ON TRUE";
        util.verifyJsonPlan(sqlQuery);
    }
}
