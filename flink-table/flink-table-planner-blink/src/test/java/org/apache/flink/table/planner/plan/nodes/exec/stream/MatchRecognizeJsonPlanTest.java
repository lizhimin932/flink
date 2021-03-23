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

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.planner.utils.StreamTableTestUtil;
import org.apache.flink.table.planner.utils.TableTestBase;

import org.junit.Before;
import org.junit.Test;

/** Test json serialization for match recognize. */
public class MatchRecognizeJsonPlanTest extends TableTestBase {
    private StreamTableTestUtil util;
    private TableEnvironment tEnv;

    @Before
    public void setup() {
        util = streamTestUtil(TableConfig.getDefault());
        tEnv = util.getTableEnv();
    }

    @Test
    public void testMatch() {
        String srcTableDdl =
                "CREATE TABLE MyTable (\n"
                        + "  id bigint,\n"
                        + "  name varchar,\n"
                        + "  proctime as PROCTIME()\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'bounded' = 'false')";
        tEnv.executeSql(srcTableDdl);
        String sinkTableDdl =
                "CREATE TABLE MySink (\n"
                        + "  a bigint,\n"
                        + "  b bigint,\n"
                        + "  c bigint\n"
                        + ") with (\n"
                        + "  'connector' = 'values',\n"
                        + "  'sink-insert-only' = 'false',\n"
                        + "  'table-sink-class' = 'DEFAULT')";
        tEnv.executeSql(sinkTableDdl);

        String sql =
                "insert into MySink"
                        + " SELECT T.aid, T.bid, T.cid\n"
                        + "     FROM MyTable MATCH_RECOGNIZE (\n"
                        + "             ORDER BY proctime\n"
                        + "             MEASURES\n"
                        + "             `A\"`.id AS aid,\n"
                        + "             \u006C.id AS bid,\n"
                        + "             C.id AS cid\n"
                        + "             PATTERN (`A\"` \u006C C)\n"
                        + "             DEFINE\n"
                        + "                 `A\"` AS name = 'a',\n"
                        + "                 \u006C AS name = 'b',\n"
                        + "                 C AS name = 'c'\n"
                        + "     ) AS T";
        util.verifyJsonPlan(sql);
    }
}
