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

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 * Accessor of {@link InternalType}s.
 */
public class InternalTypes {

	public static final StringType STRING = StringType.INSTANCE;

	public static final BooleanType BOOLEAN = BooleanType.INSTANCE;

	public static final DoubleType DOUBLE = DoubleType.INSTANCE;

	public static final FloatType FLOAT = FloatType.INSTANCE;

	public static final ByteType BYTE = ByteType.INSTANCE;

	public static final IntType INT = IntType.INSTANCE;

	public static final LongType LONG = LongType.INSTANCE;

	public static final ShortType SHORT = ShortType.INSTANCE;

	public static final CharType CHAR = CharType.INSTANCE;

	public static final ByteArrayType BYTE_ARRAY = ByteArrayType.INSTANCE;

	public static final DateType DATE = DateType.INSTANCE;

	public static final TimestampType TIMESTAMP = TimestampType.INSTANCE;

	public static final TimeType TIME = TimeType.INSTANCE;

	public static final DecimalType SYSTEM_DEFAULT_DECIMAL = DecimalType.SYSTEM_DEFAULT;

	public static ArrayType createArrayType(InternalType elementType, boolean nullable) {
		return new ArrayType(elementType, nullable);
	}

	public static DecimalType createDecimalType(int precision, int scale) {
		return new DecimalType(precision, scale);
	}

	public static MapType createMapType(InternalType keyType, InternalType valueType) {
		return new MapType(keyType, valueType);
	}

	public static <T> GenericType<T> createGenericType(Class<T> cls) {
		return new GenericType<>(cls);
	}

	public static <T> GenericType<T> createGenericType(TypeInformation<T> typeInfo) {
		return new GenericType<>(typeInfo);
	}

	public static RowType createRowType(InternalType[] types, String[] fieldNames) {
		return new RowType(types, fieldNames);
	}

	public static RowType createRowType(InternalType... types) {
		return new RowType(types);
	}

}
