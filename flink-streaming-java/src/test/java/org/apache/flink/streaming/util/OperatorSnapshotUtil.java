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

package org.apache.flink.streaming.util;

import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1Serializer;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Collection;

/**
 * Util for writing/reading {@link OperatorSubtaskState},
 * for use in tests.
 */
public class OperatorSnapshotUtil {

	public static String getResourceFilename(String filename) {
		ClassLoader cl = OperatorSnapshotUtil.class.getClassLoader();
		URL resource = cl.getResource(filename);
		return resource.getFile();
	}

	public static void writeStateHandle(OperatorSubtaskState state, String path) throws IOException {
		FileOutputStream out = new FileOutputStream(path);

		try (DataOutputStream dos = new DataOutputStream(out)) {

			// required for backwards compatibility.
			dos.writeInt(0);

			// still required for compatibility
			SavepointV1Serializer.serializeStreamStateHandle(null, dos);

			Collection<OperatorStateHandle> rawOperatorState = state.getRawOperatorState();
			if (rawOperatorState != null) {
				dos.writeInt(rawOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : rawOperatorState) {
					SavepointV1Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<OperatorStateHandle> managedOperatorState = state.getManagedOperatorState();
			if (managedOperatorState != null) {
				dos.writeInt(managedOperatorState.size());
				for (OperatorStateHandle operatorStateHandle : managedOperatorState) {
					SavepointV1Serializer.serializeOperatorStateHandle(operatorStateHandle, dos);
				}
			} else {
				// this means no states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> rawKeyedState = state.getRawKeyedState();
			if (rawKeyedState != null) {
				dos.writeInt(rawKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : rawKeyedState) {
					SavepointV1Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			Collection<KeyedStateHandle> managedKeyedState = state.getManagedKeyedState();
			if (managedKeyedState != null) {
				dos.writeInt(managedKeyedState.size());
				for (KeyedStateHandle keyedStateHandle : managedKeyedState) {
					SavepointV1Serializer.serializeKeyedStateHandle(keyedStateHandle, dos);
				}
			} else {
				// this means no operator states, not even an empty list
				dos.writeInt(-1);
			}

			dos.flush();
		}
	}

	public static OperatorSubtaskState readStateHandle(String path) throws IOException, ClassNotFoundException {
		throw new RuntimeException("break change has made for state, no longer support older version");
	}
}
