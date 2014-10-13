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

package org.apache.flink.compiler.util;

import java.io.Serializable;
import java.util.Iterator;

import org.apache.flink.api.java.record.functions.ReduceFunction;
import org.apache.flink.api.java.record.functions.FunctionAnnotation.ConstantFieldsExcept;
import org.apache.flink.types.Record;
import org.apache.flink.util.Collector;

@SuppressWarnings("deprecation")
@ConstantFieldsExcept({})
public final class IdentityReduce extends ReduceFunction implements Serializable {
	private static final long serialVersionUID = 1L;
	
	@Override
	public void reduce(Iterator<Record> records, Collector<Record> out) throws Exception {
		while (records.hasNext()) {
			out.collect(records.next());
		}
	}
}
