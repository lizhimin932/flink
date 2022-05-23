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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Arrays;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.of;

/** Tests for {@link PlannerTypeUtils#isAssignable(LogicalType, LogicalType)}. */
class LogicalTypeAssignableTest {

    private static Stream<Arguments> testData() {
        return Stream.of(
                of(new CharType(), new CharType(5), true),
                of(new CharType(), new VarCharType(5), true),
                of(new VarCharType(), new VarCharType(33), true),
                of(new BooleanType(), new BooleanType(false), true),
                of(new BinaryType(), new BinaryType(22), true),
                of(new VarBinaryType(), new VarBinaryType(44), true),
                of(new DecimalType(), new DecimalType(10, 2), true),
                of(new TinyIntType(), new TinyIntType(false), true),
                of(new SmallIntType(), new SmallIntType(false), true),
                of(new IntType(), new IntType(false), true),
                of(new BigIntType(), new BigIntType(false), true),
                of(new FloatType(), new FloatType(false), true),
                of(new DoubleType(), new DoubleType(false), true),
                of(new DateType(), new DateType(false), true),
                of(new TimeType(), new TimeType(9), false),
                of(new TimestampType(9), new TimestampType(3), true),
                of(new ZonedTimestampType(9), new ZonedTimestampType(3), false),
                of(
                        new ZonedTimestampType(false, TimestampKind.ROWTIME, 9),
                        new ZonedTimestampType(3),
                        false),
                of(
                        new YearMonthIntervalType(
                                YearMonthIntervalType.YearMonthResolution.YEAR_TO_MONTH, 2),
                        new YearMonthIntervalType(YearMonthIntervalType.YearMonthResolution.MONTH),
                        false),
                of(
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 6),
                        new DayTimeIntervalType(
                                DayTimeIntervalType.DayTimeResolution.DAY_TO_SECOND, 2, 7),
                        false),
                of(new ArrayType(new TimestampType()), new ArrayType(new SmallIntType()), false),
                of(
                        new MultisetType(new TimestampType()),
                        new MultisetType(new SmallIntType()),
                        false),
                of(
                        new MapType(new VarCharType(20), new TimestampType()),
                        new MapType(new VarCharType(99), new TimestampType()),
                        true),
                of(
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("a", new VarCharType()),
                                        new RowType.RowField("b", new VarCharType()),
                                        new RowType.RowField("c", new VarCharType()),
                                        new RowType.RowField("d", new TimestampType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowType.RowField("_a", new VarCharType()),
                                        new RowType.RowField("_b", new VarCharType()),
                                        new RowType.RowField("_c", new VarCharType()),
                                        new RowType.RowField("_d", new TimestampType()))),
                        // field name doesn't matter
                        true),
                of(
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new VarCharType()))),
                        new RowType(
                                Arrays.asList(
                                        new RowField("f1", new IntType()),
                                        new RowField("f2", new BooleanType()))),
                        false),
                of(
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new ArrayType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true),
                of(
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MapType(
                                new IntType(),
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f3", new IntType()),
                                                new RowField("f4", new IntType())))),
                        true),
                of(
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        new MultisetType(
                                new RowType(
                                        Arrays.asList(
                                                new RowField("f1", new IntType()),
                                                new RowField("f2", new IntType())))),
                        true),
                of(
                        new TypeInformationRawType<>(Types.GENERIC(PlannerTypeUtils.class)),
                        new TypeInformationRawType<>(Types.GENERIC(Object.class)),
                        false),
                of(
                        createUserType(new IntType(), new VarCharType()),
                        createUserType(new IntType(), new VarCharType()),
                        true),
                of(
                        createDistinctType(new DecimalType(10, 2)),
                        createDistinctType(new DecimalType(10, 2)),
                        true));
    }

    @ParameterizedTest(name = "{index}: [{0} COMPATIBLE {1} => {2}")
    @MethodSource("testData")
    void testAreTypesCompatible(LogicalType sourceType, LogicalType targetType, boolean equals) {
        assertThat(PlannerTypeUtils.isAssignable(sourceType, targetType)).isEqualTo(equals);
        assertThat(PlannerTypeUtils.isAssignable(sourceType, sourceType.copy())).isTrue();
        assertThat(PlannerTypeUtils.isAssignable(targetType, targetType.copy())).isTrue();
    }

    private static DistinctType createDistinctType(LogicalType sourceType) {
        return DistinctType.newBuilder(
                        ObjectIdentifier.of("cat", "db", UUID.randomUUID().toString()), sourceType)
                .description("Money type desc.")
                .build();
    }

    private static StructuredType createUserType(LogicalType... children) {
        return StructuredType.newBuilder(ObjectIdentifier.of("cat", "db", "User"), User.class)
                .attributes(
                        Arrays.stream(children)
                                .map(
                                        lt ->
                                                new StructuredType.StructuredAttribute(
                                                        UUID.randomUUID().toString(), lt))
                                .collect(Collectors.toList()))
                .description("User type desc.")
                .setFinal(true)
                .setInstantiable(true)
                .build();
    }

    private static final class User {
        public int setting;
    }
}
