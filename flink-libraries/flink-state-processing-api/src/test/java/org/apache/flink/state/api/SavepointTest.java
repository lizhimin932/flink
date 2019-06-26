/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.state.api.functions.StateBootstrapFunction;
import org.apache.flink.state.api.runtime.OperatorIDGenerator;
import org.apache.flink.state.api.runtime.metadata.SavepointMetadata;

import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Tests the api of creating new savepoints.
 */
public class SavepointTest {

	private static final String UID = "uid";

	@Test(expected = IllegalArgumentException.class)
	public void testNewSavepointEnforceUniqueUIDs() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		new NewSavepoint(new MemoryStateBackend(), 1)
			.withOperator(UID, transformation)
			.withOperator(UID, transformation);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExistingSavepointEnforceUniqueUIDs() throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		new ExistingSavepoint(env, new TestSavepointMetadata(), new MemoryStateBackend())
			.withOperator(UID, transformation)
			.withOperator(UID, transformation);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testExistingSavepointEnforceUniqueUIDsWithOldSavepoint() throws IOException {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(10);

		DataSource<Integer> input = env.fromElements(0);

		BootstrapTransformation<Integer> transformation = OperatorTransformation
			.bootstrapWith(input)
			.transform(new ExampleStateBootstrapFunction());

		new ExistingSavepoint(env, new TestSavepointMetadata(), new MemoryStateBackend())
			.withOperator(UID, transformation)
			.write("");
	}

	private static class ExampleStateBootstrapFunction extends StateBootstrapFunction<Integer> {

		@Override
		public void processElement(Integer value, Context ctx) throws Exception {
		}

		@Override
		public void snapshotState(FunctionSnapshotContext context) throws Exception {
		}

		@Override
		public void initializeState(FunctionInitializationContext context) throws Exception {
		}
	}

	private static class TestSavepointMetadata implements SavepointMetadata {

		@Override
		public int maxParallelism() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Collection<MasterState> getMasterStates() {
			throw new UnsupportedOperationException();
		}

		@Override
		public Collection<OperatorState> getOperatorStates() {
			return Collections.singletonList(new OperatorState(
				OperatorIDGenerator.fromUid(UID), 1, 4));
		}

		@Override
		public OperatorState getOperatorState(String uid) throws IOException {
			throw new UnsupportedOperationException();
		}
	}
}
