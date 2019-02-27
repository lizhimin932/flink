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

package org.apache.flink.table.type;

import org.apache.flink.types.Row;

import java.util.Arrays;

/**
 * Row type for row.
 *
 * <p>It's internal data structure is BaseRow, and it's external data structure is {@link Row}.
 */
public class RowType implements InternalType {

	/**
	 * Use DataType instead of InternalType to convert to Row (if a Pojo in Row).
	 */
	private final InternalType[] types;

	private final String[] fieldNames;

	public RowType(InternalType... types) {
		this(types, getFieldNames(types.length));
	}

	public RowType(InternalType[] types, String[] fieldNames) {
		this.types = types;
		this.fieldNames = fieldNames;
	}

	private static String[] getFieldNames(int length) {
		String[] fieldNames = new String[length];
		for (int i = 0; i < length; i++) {
			fieldNames[i] = "f" + i;
		}
		return fieldNames;
	}

	public int getArity() {
		return types.length;
	}

	public InternalType[] getFieldTypes() {
		return types;
	}

	public InternalType getTypeAt(int i) {
		return types[i];
	}

	public String[] getFieldNames() {
		return fieldNames;
	}

	public int getFieldIndex(String fieldName) {
		for (int i = 0; i < fieldNames.length; i++) {
			if (fieldNames[i].equals(fieldName)) {
				return i;
			}
		}
		return -1;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		RowType that = (RowType) o;

		// RowType comparisons should not compare names and are compatible with the behavior of CompositeTypeInfo.
		return Arrays.equals(getFieldTypes(), that.getFieldTypes());
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}

	@Override
	public String toString() {
		return "RowType{" +
				", types=" + Arrays.toString(getFieldTypes()) +
				", fieldNames=" + Arrays.toString(fieldNames) +
				'}';
	}
}
