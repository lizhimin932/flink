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

package org.apache.flink.table.planner.expressions.converter.converters;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.JsonOnNull;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;

import org.apache.calcite.sql.SqlJsonConstructorNullClause;

/** Utilities for JSON function converters. */
@Internal
class JsonConverterUtil {
    public static SqlJsonConstructorNullClause getOnNullArgument(
            CallExpression call, int argumentIdx) {
        return ((ValueLiteralExpression) call.getChildren().get(argumentIdx))
                .getValueAs(JsonOnNull.class)
                .map(JsonConverterUtil::convertOnNull)
                .orElseThrow(() -> new TableException("Missing argument for ON NULL."));
    }

    private static SqlJsonConstructorNullClause convertOnNull(JsonOnNull onNull) {
        switch (onNull) {
            case NULL:
                return SqlJsonConstructorNullClause.NULL_ON_NULL;
            case ABSENT:
                return SqlJsonConstructorNullClause.ABSENT_ON_NULL;
            default:
                throw new TableException("Unknown ON NULL behavior: " + onNull);
        }
    }

    private JsonConverterUtil() {}
}
