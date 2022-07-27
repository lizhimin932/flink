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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.changelog.ChangelogStateBackendHandle.ChangelogStateBackendHandleImpl;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** Test for {@link ChangelogTaskLocalStateStore}. */
public class ChangelogTaskLocalStateStoreTest extends TaskLocalStateStoreImplTest {

    private LocalRecoveryDirectoryProvider localRecoveryDirectoryProvider;

    @Before
    @Override
    public void before() throws Exception {
        super.before();
        this.taskLocalStateStore =
                createChangelogTaskLocalStateStore(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, subtaskIdx);
    }

    @Nonnull
    private ChangelogTaskLocalStateStore createChangelogTaskLocalStateStore(
            File[] allocationBaseDirs,
            JobID jobID,
            AllocationID allocationID,
            JobVertexID jobVertexID,
            int subtaskIdx) {
        LocalRecoveryDirectoryProviderImpl directoryProvider =
                new LocalRecoveryDirectoryProviderImpl(
                        allocationBaseDirs, jobID, jobVertexID, subtaskIdx);
        this.localRecoveryDirectoryProvider = directoryProvider;

        LocalRecoveryConfig localRecoveryConfig = new LocalRecoveryConfig(directoryProvider);
        return new ChangelogTaskLocalStateStore(
                jobID,
                allocationID,
                jobVertexID,
                subtaskIdx,
                localRecoveryConfig,
                Executors.directExecutor());
    }

    @Test
    @Override
    public void pruneCheckpoints() throws Exception {
        TestingTaskStateSnapshot stateSnapshot1 = storeChangelogStates(1, 1);
        TestingTaskStateSnapshot stateSnapshot2 = storeChangelogStates(2, 1);
        TestingTaskStateSnapshot stateSnapshot3 = storeChangelogStates(3, 1);

        taskLocalStateStore.pruneMatchingCheckpoints(id -> id != 2);
        assertNull(taskLocalStateStore.retrieveLocalState(3));
        assertTrue(stateSnapshot3.isDiscarded());
        assertNull(taskLocalStateStore.retrieveLocalState(1));
        assertTrue(stateSnapshot1.isDiscarded());
        assertTrue(checkMaterializedDirExists(1));
        assertEquals(stateSnapshot2, taskLocalStateStore.retrieveLocalState(2));
    }

    @Test
    @Override
    public void confirmCheckpoint() throws Exception {
        TestingTaskStateSnapshot stateSnapshot1 = storeChangelogStates(1, 1);
        TestingTaskStateSnapshot stateSnapshot2 = storeChangelogStates(2, 1);
        TestingTaskStateSnapshot stateSnapshot3 = storeChangelogStates(3, 1);

        taskLocalStateStore.confirmCheckpoint(3);
        assertNull(taskLocalStateStore.retrieveLocalState(2));
        assertTrue(stateSnapshot2.isDiscarded());
        assertTrue(stateSnapshot1.isDiscarded());
        assertTrue(checkMaterializedDirExists(1));
        assertEquals(stateSnapshot3, taskLocalStateStore.retrieveLocalState(3));

        TestingTaskStateSnapshot stateSnapshot4 = storeChangelogStates(4, 2);
        taskLocalStateStore.confirmCheckpoint(4);
        assertNull(taskLocalStateStore.retrieveLocalState(3));
        assertTrue(stateSnapshot3.isDiscarded());
        // delete materialization 1
        assertFalse(checkMaterializedDirExists(1));
        assertEquals(stateSnapshot4, taskLocalStateStore.retrieveLocalState(4));
    }

    @Test
    @Override
    public void abortCheckpoint() throws Exception {
        TestingTaskStateSnapshot stateSnapshot1 = storeChangelogStates(1, 1);
        TestingTaskStateSnapshot stateSnapshot2 = storeChangelogStates(2, 2);
        TestingTaskStateSnapshot stateSnapshot3 = storeChangelogStates(3, 2);
        taskLocalStateStore.abortCheckpoint(2);
        assertNull(taskLocalStateStore.retrieveLocalState(2));
        assertTrue(stateSnapshot2.isDiscarded());
        // the materialized part of checkpoint 2 retain, because it still used by checkpoint 3
        assertTrue(checkMaterializedDirExists(2));
        assertTrue(checkMaterializedDirExists(1));
        assertEquals(stateSnapshot3, taskLocalStateStore.retrieveLocalState(3));

        taskLocalStateStore.abortCheckpoint(3);
        assertFalse(checkMaterializedDirExists(2));
    }

    @Test
    public void retrievePersistedLocalStateFromDisc() {
        final TaskStateSnapshot taskStateSnapshot = createTaskStateSnapshot();
        final long checkpointId = 0L;
        taskLocalStateStore.storeLocalState(checkpointId, taskStateSnapshot);
        final ChangelogTaskLocalStateStore newTaskLocalStateStore =
                createChangelogTaskLocalStateStore(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, subtaskIdx);

        final TaskStateSnapshot retrievedTaskStateSnapshot =
                newTaskLocalStateStore.retrieveLocalState(checkpointId);

        assertThat(retrievedTaskStateSnapshot).isEqualTo(taskStateSnapshot);
    }

    @Test
    public void deletesLocalStateIfRetrievalFails() throws IOException {
        final TaskStateSnapshot taskStateSnapshot = createTaskStateSnapshot();
        final long checkpointId = 0L;
        taskLocalStateStore.storeLocalState(checkpointId, taskStateSnapshot);

        final File taskStateSnapshotFile =
                taskLocalStateStore.getTaskStateSnapshotFile(checkpointId);

        Files.write(
                taskStateSnapshotFile.toPath(), new byte[] {1, 2, 3, 4}, StandardOpenOption.WRITE);

        final ChangelogTaskLocalStateStore newTaskLocalStateStore =
                createChangelogTaskLocalStateStore(
                        allocationBaseDirs, jobID, allocationID, jobVertexID, subtaskIdx);

        assertThat(newTaskLocalStateStore.retrieveLocalState(checkpointId)).isNull();
        assertThat(taskStateSnapshotFile.getParentFile()).doesNotExist();
    }

    private boolean checkMaterializedDirExists(long materializationID) {
        File materializedDir =
                localRecoveryDirectoryProvider.subtaskSpecificCheckpointDirectory(
                        materializationID);
        return materializedDir.exists();
    }

    private void writeToMaterializedDir(long materializationID) {
        File materializedDir =
                localRecoveryDirectoryProvider.subtaskSpecificCheckpointDirectory(
                        materializationID);
        if (!materializedDir.exists() && !materializedDir.mkdirs()) {
            throw new FlinkRuntimeException(
                    String.format(
                            "Could not create the materialized directory '%s'", materializedDir));
        }
    }

    private TestingTaskStateSnapshot storeChangelogStates(
            long checkpointID, long materializationID) {
        writeToMaterializedDir(materializationID);
        OperatorID operatorID = new OperatorID();
        TestingTaskStateSnapshot taskStateSnapshot = new TestingTaskStateSnapshot();
        OperatorSubtaskState operatorSubtaskState =
                OperatorSubtaskState.builder()
                        .setManagedKeyedState(
                                new ChangelogStateBackendHandleImpl(
                                        Collections.emptyList(),
                                        Collections.emptyList(),
                                        new KeyGroupRange(0, 3),
                                        checkpointID,
                                        materializationID,
                                        checkpointID))
                        .build();
        taskStateSnapshot.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
        taskLocalStateStore.storeLocalState(checkpointID, taskStateSnapshot);
        return taskStateSnapshot;
    }
}
