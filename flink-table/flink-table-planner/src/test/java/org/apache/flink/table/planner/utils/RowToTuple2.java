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

package org.apache.flink.table.planner.utils;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import scala.Tuple2;

/** Row to scala Tuple2. */
public class RowToTuple2 implements MapFunction<Row, Tuple2<Boolean, Row>> {

    @Override
    public scala.Tuple2<Boolean, Row> map(Row value) throws Exception {
        if (value.getKind().equals(RowKind.DELETE)
                || value.getKind().equals(RowKind.UPDATE_BEFORE)) {
            return new scala.Tuple2<>(false, value);
        } else {
            return new scala.Tuple2<>(true, value);
        }
    }
}
