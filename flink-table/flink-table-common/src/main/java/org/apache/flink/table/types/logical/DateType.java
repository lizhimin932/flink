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

package org.apache.flink.table.types.logical;

import org.apache.flink.annotation.PublicEvolving;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * Logical type of a date consisting of {@code year-month-day} with values ranging from {@code 0000-01-01}
 * to {@code 9999-12-31}. Compared to the SQL standard, the range starts at year {@code 0000}.
 *
 * <p>The serialized string representation is {@code DATE}.
 *
 * <p>A conversion from and to {@code int} describes the number of days since epoch.
 */
@PublicEvolving
public final class DateType extends LogicalType {

	private static final String FORMAT = "DATE";

	private static final Set<String> NULL_OUTPUT_CONVERSION = conversionSet(
		java.sql.Date.class.getName(),
		java.time.LocalDate.class.getName());

	private static final Set<String> NOT_NULL_INPUT_OUTPUT_CONVERSION = conversionSet(
		java.sql.Date.class.getName(),
		java.time.LocalDate.class.getName(),
		int.class.getName());

	private static final Class<?> DEFAULT_CONVERSION = java.time.LocalDate.class;

	public DateType(boolean isNullable) {
		super(isNullable, LogicalTypeRoot.DATE);
	}

	public DateType() {
		this(true);
	}

	@Override
	public LogicalType copy(boolean isNullable) {
		return new DateType(isNullable);
	}

	@Override
	public String asSerializableString() {
		return withNullability(FORMAT);
	}

	@Override
	public boolean supportsInputConversion(Class<?> clazz) {
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public boolean supportsOutputConversion(Class<?> clazz) {
		if (isNullable()) {
			return NULL_OUTPUT_CONVERSION.contains(clazz.getName());
		}
		return NOT_NULL_INPUT_OUTPUT_CONVERSION.contains(clazz.getName());
	}

	@Override
	public Class<?> getDefaultConversion() {
		return DEFAULT_CONVERSION;
	}

	@Override
	public List<LogicalType> getChildren() {
		return Collections.emptyList();
	}

	@Override
	public <R> R accept(LogicalTypeVisitor<R> visitor) {
		return visitor.visit(this);
	}
}
