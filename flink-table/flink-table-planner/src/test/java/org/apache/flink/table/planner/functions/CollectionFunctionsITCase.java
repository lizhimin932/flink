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

package org.apache.flink.table.planner.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.types.Row;
import org.apache.flink.util.CollectionUtil;

import java.time.LocalDate;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.util.CollectionUtil.entry;

/** Tests for {@link BuiltInFunctionDefinitions} around arrays. */
class CollectionFunctionsITCase extends BuiltInFunctionTestBase {

    @Override
    Stream<TestSetSpec> getTestSetSpecs() {
        return Stream.of(
                        arrayContainsTestCases(),
                        arrayDistinctTestCases(),
                        arrayPositionTestCases(),
                        arrayRemoveTestCases(),
                        arrayReverseTestCases(),
                        arrayUnionTestCases(),
                        arrayConcatTestCases(),
                        arrayMaxTestCases())
                .flatMap(s -> s);
    }

    private Stream<TestSetSpec> arrayContainsTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_CONTAINS)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                null,
                                new String[] {"Hello", "World"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {1, null, 3},
                                new Integer[] {1, 2, 3})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull())
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayContains(2),
                                "ARRAY_CONTAINS(f0, 2)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f0").arrayContains(42),
                                "ARRAY_CONTAINS(f0, 42)",
                                false,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<INT> of null value
                        .testResult(
                                $("f1").arrayContains(12),
                                "ARRAY_CONTAINS(f1, 12)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f1").arrayContains(null),
                                "ARRAY_CONTAINS(f1, NULL)",
                                null,
                                DataTypes.BOOLEAN().nullable())
                        // ARRAY<STRING> NOT NULL
                        .testResult(
                                $("f2").arrayContains("Hello"),
                                "ARRAY_CONTAINS(f2, 'Hello')",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f3").arrayContains(row(true, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (TRUE, DATE '1990-10-14'))",
                                true,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(row(false, LocalDate.of(1990, 10, 14))),
                                "ARRAY_CONTAINS(f3, (FALSE, DATE '1990-10-14'))",
                                false,
                                DataTypes.BOOLEAN())
                        .testResult(
                                $("f3").arrayContains(null),
                                "ARRAY_CONTAINS(f3, null)",
                                true,
                                DataTypes.BOOLEAN())
                        // ARRAY<INT> with null elements
                        .testResult(
                                $("f4").arrayContains(null),
                                "ARRAY_CONTAINS(f4, NULL)",
                                true,
                                DataTypes.BOOLEAN().nullable())
                        .testResult(
                                $("f5").arrayContains(lit(null, DataTypes.INT())),
                                "ARRAY_CONTAINS(f5, CAST(NULL AS INT))",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f5").arrayContains(lit(4, DataTypes.INT().notNull())),
                                "ARRAY_CONTAINS(f5, 4)",
                                false,
                                DataTypes.BOOLEAN().notNull())
                        .testResult(
                                $("f5").arrayContains(lit(3, DataTypes.INT().notNull())),
                                "ARRAY_CONTAINS(f5, 3)",
                                true,
                                DataTypes.BOOLEAN().notNull())
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_CONTAINS(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)")
                        .testTableApiValidationError(
                                $("f0").arrayContains(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONTAINS(haystack <ARRAY>, needle <ARRAY ELEMENT>)"));
    }

    private Stream<TestSetSpec> arrayDistinctTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_DISTINCT)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 3},
                                new Integer[] {null, 1, 2, 3, 4, 5, 4, 3, 2, 1, null},
                                null,
                                new String[] {"Hello", "Hello", "Hello"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull(),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        .testResult(
                                $("f0").arrayDistinct(),
                                "ARRAY_DISTINCT(f0)",
                                new Integer[] {1, 2, 3},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f1").arrayDistinct(),
                                "ARRAY_DISTINCT(f1)",
                                new Integer[] {null, 1, 2, 3, 4, 5},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f2").arrayDistinct(),
                                "ARRAY_DISTINCT(f2)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f3").arrayDistinct(),
                                "ARRAY_DISTINCT(f3)",
                                new String[] {"Hello"},
                                DataTypes.ARRAY(DataTypes.STRING()).notNull())
                        .testResult(
                                $("f4").arrayDistinct(),
                                "ARRAY_DISTINCT(f4)",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE()))));
    }

    private Stream<TestSetSpec> arrayPositionTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_POSITION)
                        .onFieldsWithData(
                                new Integer[] {null, 1, 2, 2, null},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[][] {
                                    new Integer[] {1, null, 3}, new Integer[] {0}, new Integer[] {1}
                                },
                                new Map[] {
                                    null,
                                    CollectionUtil.map(entry(1, "a"), entry(2, "b")),
                                    CollectionUtil.map(entry(3, "c"), entry(4, "d")),
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT().notNull()).notNull(),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                        .testResult(
                                $("f0").arrayPosition(lit(2, DataTypes.INT().notNull())),
                                "ARRAY_POSITION(f0, 2)",
                                3,
                                DataTypes.INT().notNull())
                        .testResult(
                                $("f0").arrayPosition(null),
                                "ARRAY_POSITION(f0, NULL)",
                                null,
                                DataTypes.INT())
                        .testResult(
                                $("f1").arrayPosition(2),
                                "ARRAY_POSITION(f1, 2)",
                                null,
                                DataTypes.INT())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f2").arrayPosition(row(true, LocalDate.of(1990, 10, 14))),
                                "ARRAY_POSITION(f2, (TRUE, DATE '1990-10-14'))",
                                2,
                                DataTypes.INT())
                        // ARRAY<ARRAY<INT>>
                        .testResult(
                                $("f3").arrayPosition(new Integer[] {0, 1}),
                                "ARRAY_POSITION(f3, ARRAY[0, 1])",
                                0,
                                DataTypes.INT())
                        // ARRAY<MAP<INT, STRING>>
                        .testResult(
                                $("f4").arrayPosition(
                                                CollectionUtil.map(entry(3, "c"), entry(4, "d"))),
                                "ARRAY_POSITION(f4, MAP[3, 'c', 4, 'd'])",
                                3,
                                DataTypes.INT())
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_POSITION(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_POSITION(haystack <ARRAY>, needle <ARRAY ELEMENT>)")
                        .testTableApiValidationError(
                                $("f0").arrayPosition(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_POSITION(haystack <ARRAY>, needle <ARRAY ELEMENT>)"));
    }

    private Stream<TestSetSpec> arrayRemoveTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REMOVE)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 2},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {null, null, 1},
                                new Integer[][] {
                                    new Integer[] {1, null, 3}, new Integer[] {0}, new Integer[] {1}
                                },
                                new Map[] {
                                    CollectionUtil.map(entry(1, "a"), entry(2, "b")),
                                    CollectionUtil.map(entry(3, "c"), entry(4, "d")),
                                    null
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())))
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayRemove(2),
                                "ARRAY_REMOVE(f0, 2)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f0").arrayRemove(42),
                                "ARRAY_REMOVE(f0, 42)",
                                new Integer[] {1, 2, 2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f0").arrayRemove(
                                                lit(null, DataTypes.SMALLINT())
                                                        .cast(DataTypes.INT())),
                                "ARRAY_REMOVE(f0, CAST(NULL AS INT))",
                                new Integer[] {1, 2, 2},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // ARRAY<INT> of NULL value
                        .testResult(
                                $("f1").arrayRemove(12),
                                "ARRAY_REMOVE(f1, 12)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f1").arrayRemove(null),
                                "ARRAY_REMOVE(f1, NULL)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f2").arrayRemove(row(true, LocalDate.of(1990, 10, 14))),
                                "ARRAY_REMOVE(f2, (TRUE, DATE '1990-10-14'))",
                                new Row[] {Row.of(true, LocalDate.of(2022, 4, 20)), null},
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.BOOLEAN(), DataTypes.DATE()))
                                        .nullable())
                        .testResult(
                                $("f2").arrayRemove(null),
                                "ARRAY_REMOVE(f2, NULL)",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                },
                                DataTypes.ARRAY(
                                                DataTypes.ROW(
                                                        DataTypes.BOOLEAN(), DataTypes.DATE()))
                                        .nullable())
                        // ARRAY<INT> with NULL elements
                        .testResult(
                                $("f3").arrayRemove(null),
                                "ARRAY_REMOVE(f3, NULL)",
                                new Integer[] {1},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        // ARRAY<ARRAY<INT>>
                        .testResult(
                                $("f4").arrayRemove(new Integer[] {0}),
                                "ARRAY_REMOVE(f4, ARRAY[0])",
                                new Integer[][] {new Integer[] {1, null, 3}, new Integer[] {1}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT()).nullable()))
                        // ARRAY<MAP<INT, STRING>> with NULL elements
                        .testResult(
                                $("f5").arrayRemove(
                                                CollectionUtil.map(entry(3, "c"), entry(4, "d"))),
                                "ARRAY_REMOVE(f5, MAP[3, 'c', 4, 'd'])",
                                new Map[] {CollectionUtil.map(entry(1, "a"), entry(2, "b")), null},
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING()))
                                        .nullable())
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_REMOVE(f0, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_REMOVE(haystack <ARRAY>, needle <ARRAY ELEMENT>)")
                        .testTableApiValidationError(
                                $("f0").arrayRemove(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_REMOVE(haystack <ARRAY>, needle <ARRAY ELEMENT>)"));
    }

    private Stream<TestSetSpec> arrayReverseTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_REVERSE)
                        .onFieldsWithData(
                                new Integer[] {1, 2, 2, null},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                })
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        .testResult(
                                $("f0").arrayReverse(),
                                "ARRAY_REVERSE(f0)",
                                new Integer[] {null, 2, 2, 1},
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f1").arrayReverse(),
                                "ARRAY_REVERSE(f1)",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()).nullable())
                        .testResult(
                                $("f2").arrayReverse(),
                                "ARRAY_REVERSE(f2)",
                                new Row[] {
                                    null,
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE()))));
    }

    private Stream<TestSetSpec> arrayUnionTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_UNION)
                        .onFieldsWithData(
                                new Integer[] {1, 2, null},
                                new Integer[] {1},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                1)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT().notNull()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.INT())
                        // ARRAY<INT>
                        .testResult(
                                $("f0").arrayUnion(new Integer[] {1, null, 4}),
                                "ARRAY_UNION(f0, ARRAY[1, NULL, 4])",
                                new Integer[] {1, 2, null, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f1").arrayUnion(new Integer[] {2, null}),
                                "ARRAY_UNION(f1, ARRAY[2, NULL])",
                                new Integer[] {1, 2, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        // insert cast bug https://issues.apache.org/jira/browse/CALCITE-5674.
                        //                        .testResult(
                        //                                $("f0").arrayUnion(array(1.0d, null,
                        // 4.0d)),
                        //                                "ARRAY_UNION(f0, ARRAY[1.0E0, NULL,
                        // 4.0E0])",
                        //                                new Double[] {1.0d, 2.0d, null, 4.0d},
                        //                                DataTypes.ARRAY(DataTypes.DOUBLE()))
                        // ARRAY<INT> of null value
                        .testResult(
                                $("f2").arrayUnion(new Integer[] {1, null, 4}),
                                "ARRAY_UNION(f2, ARRAY[1, NULL, 4])",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        // ARRAY<ROW<BOOLEAN, DATE>>
                        .testResult(
                                $("f3").arrayUnion(
                                                new Row[] {
                                                    null, Row.of(true, LocalDate.of(1990, 10, 14)),
                                                }),
                                "ARRAY_UNION(f3, ARRAY[NULL, (TRUE, DATE '1990-10-14')])",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        // invalid signatures
                        .testSqlValidationError(
                                "ARRAY_UNION(f4, TRUE)",
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_UNION(<COMMON>, <COMMON>)")
                        .testTableApiValidationError(
                                $("f4").arrayUnion(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_UNION(<COMMON>, <COMMON>)"));
    }

    private Stream<TestSetSpec> arrayConcatTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_CONCAT)
                        .onFieldsWithData(
                                new Integer[] {1, 2, null},
                                null,
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Integer[] {1},
                                1,
                                new Integer[][] {{1}},
                                new String[] {"123"})
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.INT().notNull()),
                                DataTypes.INT().notNull(),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())).notNull(),
                                DataTypes.ARRAY(DataTypes.STRING()).notNull())
                        .testResult(
                                $("f0").arrayConcat(new Integer[] {1, null, 4}),
                                "ARRAY_CONCAT(f0, ARRAY[1, NULL, 4])",
                                new Integer[] {1, 2, null, 1, null, 4},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f0").arrayConcat(),
                                "ARRAY_CONCAT(f0)",
                                new Integer[] {1, 2, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").arrayConcat(
                                                new Integer[] {null, null, null},
                                                new Integer[] {1, 2, 3},
                                                new Integer[] {3, 4, 5}),
                                "Invalid function call:\n" + "array(NULL, NULL, NULL)")
                        .testResult(
                                $("f1").arrayConcat(
                                                new Integer[] {1, null, 4},
                                                new Integer[] {2, 3, 4},
                                                new Integer[] {2, 3, 4}),
                                "ARRAY_CONCAT(f1, ARRAY[1, NULL, 4], ARRAY[2, 3, 4], ARRAY[2, 3, 4])",
                                null,
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testResult(
                                $("f2").arrayConcat(
                                                new Row[] {
                                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                                },
                                                new Row[] {
                                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                                }),
                                "ARRAY_CONCAT(f2, ARRAY[(TRUE, DATE '1990-10-14')], ARRAY[(TRUE, DATE '1990-10-14')])",
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null,
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    Row.of(true, LocalDate.of(1990, 10, 14))
                                },
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())))
                        .testResult(
                                $("f3").arrayConcat(new Integer[] {2, null}),
                                "ARRAY_CONCAT(f3, ARRAY[2, NULL])",
                                new Integer[] {1, 2, null},
                                DataTypes.ARRAY(DataTypes.INT()))
                        .testTableApiValidationError(
                                $("f0").arrayConcat(
                                                new Integer[] {null, null, null},
                                                new Integer[] {1, 2, 3},
                                                new Integer[] {3, 4, 5}),
                                "Invalid function call:\n" + "array(NULL, NULL, NULL)")
                        .testTableApiValidationError(
                                $("f4").arrayConcat(true),
                                "Invalid input arguments. Expected signatures are:\n"
                                        + "ARRAY_CONCAT(<COMMON>, <COMMON>...)")
                        .testTableApiValidationError(
                                $("f5").arrayConcat(new Integer[] {1}),
                                "Invalid function call:\n"
                                        + "ARRAY_CONCAT(ARRAY<ARRAY<INT>> NOT NULL, ARRAY<INT NOT NULL> NOT NULL)")
                        .testTableApiValidationError(
                                $("f6").arrayConcat(new Integer[] {123}),
                                "Invalid function call:\n"
                                        + "ARRAY_CONCAT(ARRAY<STRING> NOT NULL, ARRAY<INT NOT NULL> NOT NULL)"));
    }

    private Stream<TestSetSpec> arrayMaxTestCases() {
        return Stream.of(
                TestSetSpec.forFunction(BuiltInFunctionDefinitions.ARRAY_MAX)
                        .onFieldsWithData(
                                new Integer[] {1, 2, null},
                                null,
                                new Double[] {1.2, null, 3.4, 8.0},
                                new String[] {"a", null, "bc", "d", "def"},
                                new Row[] {
                                    Row.of(true, LocalDate.of(2022, 4, 20)),
                                    Row.of(true, LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Map[] {
                                    CollectionUtil.map(entry(1, "a"), entry(2, "b")),
                                    CollectionUtil.map(entry(3, "c"), entry(4, "d")),
                                    null
                                },
                                new Integer[][] {{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, null},
                                new Row[] {
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    Row.of(LocalDate.of(2022, 4, 20)),
                                    Row.of(LocalDate.of(1990, 10, 14)),
                                    null
                                },
                                new Boolean[] {true, false, true, false, true, null},
                                new Row[] {
                                    Row.of(true),
                                    Row.of(false),
                                    Row.of(true),
                                    Row.of(false),
                                    Row.of(true),
                                    Row.of(false),
                                    null
                                },
                                new Row[] {
                                    Row.of(1), Row.of(2), Row.of(8), Row.of(4), Row.of(5),
                                    Row.of(8), null
                                },
                                1,
                                new Integer[][] {{1, 2}, {2, 3}, null},
                                new LocalDate[] {
                                    LocalDate.of(2022, 1, 2),
                                    LocalDate.of(2023, 4, 21),
                                    LocalDate.of(2022, 12, 24),
                                    LocalDate.of(2026, 2, 10),
                                    LocalDate.of(2012, 5, 16),
                                    LocalDate.of(2092, 7, 19)
                                },
                                null)
                        .andDataTypes(
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.INT()),
                                DataTypes.ARRAY(DataTypes.DOUBLE()),
                                DataTypes.ARRAY(DataTypes.STRING()),
                                DataTypes.ARRAY(
                                        DataTypes.ROW(DataTypes.BOOLEAN(), DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.MAP(DataTypes.INT(), DataTypes.STRING())),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.DATE())),
                                DataTypes.ARRAY(DataTypes.BOOLEAN()),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.BOOLEAN())),
                                DataTypes.ARRAY(DataTypes.ROW(DataTypes.INT())),
                                DataTypes.INT().notNull(),
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.INT())),
                                DataTypes.ARRAY(DataTypes.DATE()),
                                DataTypes.ARRAY(DataTypes.INT().notNull()))
                        .testResult($("f0").arrayMax(), "ARRAY_MAX(f0)", 2, DataTypes.INT())
                        .testResult($("f1").arrayMax(), "ARRAY_MAX(f1)", null, DataTypes.INT())
                        .testResult($("f2").arrayMax(), "ARRAY_MAX(f2)", 8.0, DataTypes.DOUBLE())
                        .testResult($("f3").arrayMax(), "ARRAY_MAX(f3)", "def", DataTypes.STRING())
                        .testResult($("f14").arrayMax(), "ARRAY_MAX(f1)", null, DataTypes.INT())
                        .testResult(
                                $("f13").arrayMax(),
                                "ARRAY_MAX(f13)",
                                LocalDate.of(2092, 7, 19),
                                DataTypes.DATE())
                        .testSqlValidationError(
                                "ARRAY_MAX(f4)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ROW<`f0` BOOLEAN, `f1` DATE>>")
                        .testTableApiValidationError(
                                $("f4").arrayMax(),
                                "Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ROW<`f0` BOOLEAN, `f1` DATE>>")
                        .testSqlValidationError(
                                "ARRAY_MAX(f5)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<MAP<INT, STRING>>")
                        .testTableApiValidationError(
                                $("f5").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<MAP<INT, STRING>>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f6)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ARRAY<INT>>)")
                        .testTableApiValidationError(
                                $("f6").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<ARRAY<INT>>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f7)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ROW<`f0` DATE>>)")
                        .testTableApiValidationError(
                                $("f7").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<ROW<`f0` DATE>>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f8)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<BOOLEAN>)")
                        .testTableApiValidationError(
                                $("f8").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<BOOLEAN>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f9)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ROW<`f0` BOOLEAN>>)")
                        .testTableApiValidationError(
                                $("f9").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<ROW<`f0` BOOLEAN>>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f10)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ROW<`f0` INT>>)")
                        .testTableApiValidationError(
                                $("f10").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<ROW<`f0` INT>>)")
                        .testTableApiValidationError(
                                $("f11").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(INT NOT NULL)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f11)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(INT NOT NULL)")
                        .testTableApiValidationError(
                                $("f12").arrayMax(),
                                "Invalid function call:\n" + "ARRAY_MAX(ARRAY<ARRAY<INT>>)")
                        .testSqlValidationError(
                                "ARRAY_MAX(f12)",
                                "SQL validation failed. Invalid function call:\n"
                                        + "ARRAY_MAX(ARRAY<ARRAY<INT>>)"));
    }
}
