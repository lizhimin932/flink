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

package org.apache.flink.table.planner.functions.sql;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.functions.casting.CastRuleProvider;
import org.apache.flink.table.types.logical.LogicalType;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperatorBinding;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import static org.apache.flink.table.functions.BuiltInFunctionDefinition.DEFAULT_VERSION;

/**
 * This class implements the {@code TRY_CAST} built-in, essentially delegating all the method
 * invocations, whenever is possible, to Calcite's {@link SqlCastFunction}.
 */
@Internal
public class SqlTryCastFunction extends BuiltInSqlFunction {

    /**
     * Note that this constructor is mimicking as much as possible the constructor of Calcite's
     * {@link SqlCastFunction}.
     */
    SqlTryCastFunction() {
        super(
                "TRY_CAST",
                DEFAULT_VERSION,
                SqlKind.OTHER_FUNCTION,
                null,
                SqlStdOperatorTable.CAST
                        .getOperandTypeInference(), // From Calcite's SqlCastFunction
                null,
                SqlFunctionCategory.SYSTEM,
                true,
                false,
                SqlStdOperatorTable.CAST::getMonotonicity);
    }

    @Override
    public String getSignatureTemplate(final int operandsCount) {
        return SqlStdOperatorTable.CAST.getSignatureTemplate(operandsCount);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlStdOperatorTable.CAST.getOperandCountRange();
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        return SqlStdOperatorTable.CAST.checkOperandTypes(callBinding, throwOnFailure);
    }

    @Override
    public SqlSyntax getSyntax() {
        return SqlSyntax.SPECIAL;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        // Taken from SqlCastFunction, but using the name of this operator
        assert call.operandCount() == 2;
        final SqlWriter.Frame frame = writer.startFunCall(getName());
        call.operand(0).unparse(writer, 0, 0);
        writer.sep("AS");
        if (call.operand(1) instanceof SqlIntervalQualifier) {
            writer.sep("INTERVAL");
        }
        call.operand(1).unparse(writer, 0, 0);
        writer.endFunCall(frame);
    }

    @Override
    public RelDataType inferReturnType(SqlOperatorBinding opBinding) {
        RelDataType returnType = SqlStdOperatorTable.CAST.inferReturnType(opBinding);

        final LogicalType fromLogicalType =
                FlinkTypeFactory.toLogicalType(opBinding.getOperandType(0));
        final LogicalType toLogicalType =
                FlinkTypeFactory.toLogicalType(opBinding.getOperandType(1));

        // This is nullable only and only if the cast rule can fail
        if (CastRuleProvider.canFail(fromLogicalType, toLogicalType)) {
            returnType = opBinding.getTypeFactory().createTypeWithNullability(returnType, true);
        }

        return returnType;
    }
}
