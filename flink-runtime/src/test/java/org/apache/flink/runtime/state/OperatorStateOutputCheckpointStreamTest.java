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

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.Map;

public class OperatorStateOutputCheckpointStreamTest {

    private static final int STREAM_CAPACITY = 128;

    private static OperatorStateCheckpointOutputStream createStream() throws IOException {
        CheckpointStreamFactory.CheckpointStateOutputStream checkStream =
                new TestMemoryCheckpointOutputStream(STREAM_CAPACITY);
        return new OperatorStateCheckpointOutputStream(checkStream);
    }

    private OperatorStateHandle writeAllTestKeyGroups(
            OperatorStateCheckpointOutputStream stream, int numPartitions) throws Exception {

        DataOutputView dov = new DataOutputViewStreamWrapper(stream);
        for (int i = 0; i < numPartitions; ++i) {
            Assertions.assertEquals(i, stream.getNumberOfPartitions());
            stream.startNewPartition();
            dov.writeInt(i);
        }

        return stream.closeAndGetHandle();
    }

    @Test
    public void testCloseNotPropagated() throws Exception {
        OperatorStateCheckpointOutputStream stream = createStream();
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        stream.close();
        Assertions.assertFalse(innerStream.isClosed());
        innerStream.close();
    }

    @Test
    public void testEmptyOperatorStream() throws Exception {
        OperatorStateCheckpointOutputStream stream = createStream();
        TestMemoryCheckpointOutputStream innerStream =
                (TestMemoryCheckpointOutputStream) stream.getDelegate();
        OperatorStateHandle emptyHandle = stream.closeAndGetHandle();
        Assertions.assertTrue(innerStream.isClosed());
        Assertions.assertEquals(0, stream.getNumberOfPartitions());
        Assertions.assertEquals(null, emptyHandle);
    }

    @Test
    public void testWriteReadRoundtrip() throws Exception {
        int numPartitions = 3;
        OperatorStateCheckpointOutputStream stream = createStream();
        OperatorStateHandle fullHandle = writeAllTestKeyGroups(stream, numPartitions);
        Assertions.assertNotNull(fullHandle);

        Map<String, OperatorStateHandle.StateMetaInfo> stateNameToPartitionOffsets =
                fullHandle.getStateNameToPartitionOffsets();
        for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry :
                stateNameToPartitionOffsets.entrySet()) {

            Assertions.assertEquals(
                    OperatorStateHandle.Mode.SPLIT_DISTRIBUTE,
                    entry.getValue().getDistributionMode());
        }
        verifyRead(fullHandle, numPartitions);
    }

    private static void verifyRead(OperatorStateHandle fullHandle, int numPartitions)
            throws IOException {
        int count = 0;
        try (FSDataInputStream in = fullHandle.openInputStream()) {
            OperatorStateHandle.StateMetaInfo metaInfo =
                    fullHandle
                            .getStateNameToPartitionOffsets()
                            .get(DefaultOperatorStateBackend.DEFAULT_OPERATOR_STATE_NAME);

            long[] offsets = metaInfo.getOffsets();

            Assertions.assertNotNull(offsets);

            DataInputView div = new DataInputViewStreamWrapper(in);
            for (int i = 0; i < numPartitions; ++i) {
                in.seek(offsets[i]);
                Assertions.assertEquals(i, div.readInt());
                ++count;
            }
        }

        Assertions.assertEquals(numPartitions, count);
    }
}
