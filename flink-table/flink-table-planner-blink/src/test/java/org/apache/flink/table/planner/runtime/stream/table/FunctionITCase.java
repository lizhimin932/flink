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

package org.apache.flink.table.planner.runtime.stream.table;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.utils.TestCollectionTableFactory;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBase;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for catalog and system in stream table environment.
 */
public class FunctionITCase extends StreamingTestBase {

	@Test
	public void testPrimitiveScalarFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of(1, 1L, 1L),
			Row.of(2, 2L, 1L),
			Row.of(3, 3L, 1L)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of(1, 2L, 1L),
			Row.of(2, 4L, 1L),
			Row.of(3, 6L, 1L)
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().sqlUpdate("CREATE TABLE TestTable(a INT, b BIGINT, c BIGINT) WITH ('connector' = 'COLLECTION')");

		tEnv().from("TestTable")
			.select(
				$("a"),
				call(new SimpleScalarFunction(), $("a"), $("b")),
				call(new SimpleScalarFunction(), $("a"), $("b"))
					.plus(1)
					.minus(call(new SimpleScalarFunction(), $("a"), $("b")))
			)
			.insertInto("TestTable");
		tEnv().execute("Test Job");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	@Test
	public void testRowTableFunction() throws Exception {
		final List<Row> sourceData = Arrays.asList(
			Row.of("1,2,3"),
			Row.of("2,3,4"),
			Row.of("3,4,5"),
			Row.of((String) null)
		);

		final List<Row> sinkData = Arrays.asList(
			Row.of("1,2,3", new String[]{"1", "2", "3"}),
			Row.of("2,3,4", new String[]{"2", "3", "4"}),
			Row.of("3,4,5", new String[]{"3", "4", "5"})
		);

		TestCollectionTableFactory.reset();
		TestCollectionTableFactory.initData(sourceData);

		tEnv().sqlUpdate("CREATE TABLE SourceTable(s STRING) WITH ('connector' = 'COLLECTION')");
		tEnv().sqlUpdate("CREATE TABLE SinkTable(s STRING, sa ARRAY<STRING>) WITH ('connector' = 'COLLECTION')");

		tEnv().from("SourceTable")
			.joinLateral(call(new SimpleTableFunction(), $("s")).as("a", "b"))
			.select($("a"), $("b"))
			.insertInto("SinkTable");
		tEnv().execute("Test Job");

		assertThat(TestCollectionTableFactory.getResult(), equalTo(sinkData));
	}

	// --------------------------------------------------------------------------------------------
	// Test functions
	// --------------------------------------------------------------------------------------------

	/**
	 * Simple scalar function.
	 */
	public static class SimpleScalarFunction extends ScalarFunction {
		public Long eval(Integer i, Long j) {
			return i + j;
		}
	}

	/**
	 * Table function that returns a row.
	 */
	@FunctionHint(output = @DataTypeHint("ROW<s STRING, sa ARRAY<STRING>>"))
	public static class SimpleTableFunction extends TableFunction<Row> {
		public void eval(String s) {
			if (s == null) {
				collect(null);
			} else {
				collect(Row.of(s, s.split(",")));
			}
		}
	}
}
