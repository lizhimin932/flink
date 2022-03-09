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

package org.apache.flink.table.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.util.Preconditions;

import java.util.Objects;
import java.util.Set;

/**
 * A "marker" function definition of an user-defined table function that uses the old type system
 * stack.
 *
 * <p>This class can be dropped once we introduce a new type inference.
 *
 * @deprecated Non-legacy functions can simply omit this wrapper for declarations.
 */
@Deprecated
public final class TableFunctionDefinition implements FunctionDefinition {

    private final String name;
    private final TableFunction<?> tableFunction;
    private final TypeInformation<?> resultType;

    public TableFunctionDefinition(
            String name, TableFunction<?> tableFunction, TypeInformation<?> resultType) {
        this.name = Preconditions.checkNotNull(name);
        this.tableFunction = Preconditions.checkNotNull(tableFunction);
        this.resultType = Preconditions.checkNotNull(resultType);
    }

    public String getName() {
        return name;
    }

    public TableFunction<?> getTableFunction() {
        return tableFunction;
    }

    public TypeInformation<?> getResultType() {
        return resultType;
    }

    @Override
    public FunctionKind getKind() {
        return FunctionKind.TABLE;
    }

    @Override
    public TypeInference getTypeInference(DataTypeFactory typeFactory) {
        throw new TableException(
                "Functions implemented for the old type system are not supported.");
    }

    @Override
    public Set<FunctionRequirement> getRequirements() {
        return tableFunction.getRequirements();
    }

    @Override
    public boolean isDeterministic() {
        return tableFunction.isDeterministic();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TableFunctionDefinition that = (TableFunctionDefinition) o;
        return name.equals(that.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name);
    }

    @Override
    public String toString() {
        return name;
    }
}
