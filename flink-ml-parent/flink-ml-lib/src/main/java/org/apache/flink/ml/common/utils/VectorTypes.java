/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.common.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.ml.common.linalg.DenseVector;
import org.apache.flink.ml.common.linalg.SparseVector;
import org.apache.flink.ml.common.linalg.Vector;

import org.apache.flink.shaded.guava18.com.google.common.collect.HashBiMap;

/**
 * Built-in vector types. <br/>
 * This class contains bi-direction mapping between <code>TypeInformation</code>s and their names.
 */
public class VectorTypes {
	private static final HashBiMap<String, TypeInformation> TYPES = HashBiMap.create();

	/**
	 * <code>DenseVector</code> type information.
	 */
	public static final TypeInformation<DenseVector> DENSE_VECTOR = TypeInformation.of(DenseVector.class);

	/**
	 * <code>SparseVector</code> type information.
	 */
	public static final TypeInformation<SparseVector> SPARSE_VECTOR = TypeInformation.of(SparseVector.class);

	/**
	 * <code>Vector</code> type information.
	 * For efficiency, use type information of sub-class <code>DenseVector</code> and <code>SparseVector</code>
	 * as much as possible. When an operator output both sub-class type of vectors, use this one.
	 */
	public static final TypeInformation<Vector> VECTOR = TypeInformation.of(Vector.class);

	static {
		TYPES.put("VEC_TYPES_DENSE_VECTOR", DENSE_VECTOR);
		TYPES.put("VEC_TYPES_SPARSE_VECTOR", SPARSE_VECTOR);
		TYPES.put("VEC_TYPES_VECTOR", VECTOR);
	}

	/**
	 * Get type name from <code>TypeInformation</code>.
	 *
	 * @param type <code>TypeInformation</code>
	 * @return Corresponding type name, or null if  not found.
	 */
	public static String getTypeName(TypeInformation type) {
		return TYPES.inverse().get(type);
	}

	/**
	 * Get <code>TypeInformation</code> from type name.
	 *
	 * @param name type name string.
	 * @return Corresponding <code>TypeInformation</code>, or null if not found.
	 */
	public static TypeInformation getTypeInformation(String name) {
		return TYPES.get(name);
	}
}
