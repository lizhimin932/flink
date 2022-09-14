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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.inference.CallContext;
import org.apache.flink.table.types.inference.TypeStrategy;

import java.util.Optional;

/** Type strategy of {@code TO_TIMESTAMP_LTZ}. */
@Internal
public class ToTimestampLtzTypeStrategy implements TypeStrategy {

    @Override
    public Optional<DataType> inferType(CallContext callContext) {
        if (callContext.isArgumentLiteral(1)) {
            final int precision = callContext.getArgumentValue(1, Integer.class).get();
            return Optional.of(DataTypes.TIMESTAMP_LTZ(precision));
        }
        return Optional.of(DataTypes.TIMESTAMP_LTZ(3));
    }
}
