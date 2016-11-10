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

package org.apache.flink.runtime.deployment;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionEdge;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.instance.Instance;
import org.apache.flink.runtime.instance.InstanceConnectionInfo;
import org.apache.flink.runtime.instance.SimpleSlot;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.junit.Test;

import java.net.InetAddress;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InputChannelDeploymentDescriptorTest {

	/**
	 * Tests the deployment descriptors for local, remote, and unknown partition
	 * locations (with lazy deployment allowed and all execution states for the
	 * producers).
	 */
	@Test
	public void testMixedLocalRemoteUnknownDeployment() throws Exception {
		boolean allowLazyDeployment = true;

		ExecutionVertex consumer = mock(ExecutionVertex.class);
		SimpleSlot consumerSlot = mockSlot(createConnInfo(5000));

		// Local and remote channel are only allowed for certain execution
		// states.
		for (ExecutionState state : ExecutionState.values()) {
			// Local partition
			ExecutionVertex localProducer = mockExecutionVertex(state, consumerSlot);
			IntermediateResultPartition localPartition = mockPartition(localProducer);
			ResultPartitionID localPartitionId = new ResultPartitionID(localPartition.getPartitionId(), localProducer.getCurrentExecutionAttempt().getAttemptId());
			ExecutionEdge localEdge = new ExecutionEdge(localPartition, consumer, 0);

			// Remote partition
			InstanceConnectionInfo connInfo = createConnInfo(6000);
			ExecutionVertex remoteProducer = mockExecutionVertex(state, mockSlot(connInfo)); // new slot
			IntermediateResultPartition remotePartition = mockPartition(remoteProducer);
			ResultPartitionID remotePartitionId = new ResultPartitionID(remotePartition.getPartitionId(), remoteProducer.getCurrentExecutionAttempt().getAttemptId());
			ConnectionID remoteConnectionId = new ConnectionID(connInfo, 0);
			ExecutionEdge remoteEdge = new ExecutionEdge(remotePartition, consumer, 1);

			// Unknown partition
			ExecutionVertex unknownProducer = mockExecutionVertex(state, null); // no assigned resource
			IntermediateResultPartition unknownPartition = mockPartition(unknownProducer);
			ResultPartitionID unknownPartitionId = new ResultPartitionID(unknownPartition.getPartitionId(), unknownProducer.getCurrentExecutionAttempt().getAttemptId());
			ExecutionEdge unknownEdge = new ExecutionEdge(unknownPartition, consumer, 2);

			InputChannelDeploymentDescriptor[] desc = InputChannelDeploymentDescriptor.fromEdges(
				new ExecutionEdge[]{localEdge, remoteEdge, unknownEdge},
				consumerSlot,
				allowLazyDeployment);

			assertEquals(3, desc.length);

			// These states are allowed
			if (state == ExecutionState.RUNNING || state == ExecutionState.FINISHED ||
				state == ExecutionState.SCHEDULED || state == ExecutionState.DEPLOYING) {

				// Create local or remote channels
				assertEquals(localPartitionId, desc[0].getConsumedPartitionId());
				assertTrue(desc[0].getConsumedPartitionLocation().isLocal());
				assertNull(desc[0].getConsumedPartitionLocation().getConnectionId());

				assertEquals(remotePartitionId, desc[1].getConsumedPartitionId());
				assertTrue(desc[1].getConsumedPartitionLocation().isRemote());
				assertEquals(remoteConnectionId, desc[1].getConsumedPartitionLocation().getConnectionId());
			} else {
				// Unknown (lazy deployment allowed)
				assertEquals(localPartitionId, desc[0].getConsumedPartitionId());
				assertTrue(desc[0].getConsumedPartitionLocation().isUnknown());
				assertNull(desc[0].getConsumedPartitionLocation().getConnectionId());

				assertEquals(remotePartitionId, desc[1].getConsumedPartitionId());
				assertTrue(desc[1].getConsumedPartitionLocation().isUnknown());
				assertNull(desc[1].getConsumedPartitionLocation().getConnectionId());
			}

			assertEquals(unknownPartitionId, desc[2].getConsumedPartitionId());
			assertTrue(desc[2].getConsumedPartitionLocation().isUnknown());
			assertNull(desc[2].getConsumedPartitionLocation().getConnectionId());
		}
	}

	@Test
	public void testUnknownChannelWithoutLazyDeploymentThrows() throws Exception {
		ExecutionVertex consumer = mock(ExecutionVertex.class);
		SimpleSlot consumerSlot = mock(SimpleSlot.class);

		// Unknown partition
		ExecutionVertex unknownProducer = mockExecutionVertex(ExecutionState.CREATED, null); // no assigned resource
		IntermediateResultPartition unknownPartition = mockPartition(unknownProducer);
		ResultPartitionID unknownPartitionId = new ResultPartitionID(unknownPartition.getPartitionId(), unknownProducer.getCurrentExecutionAttempt().getAttemptId());
		ExecutionEdge unknownEdge = new ExecutionEdge(unknownPartition, consumer, 2);

		// This should work if lazy deployment is allowed
		boolean allowLazyDeployment = true;

		InputChannelDeploymentDescriptor[] desc = InputChannelDeploymentDescriptor.fromEdges(
			new ExecutionEdge[]{unknownEdge},
			consumerSlot,
			allowLazyDeployment);

		assertEquals(1, desc.length);

		assertEquals(unknownPartitionId, desc[0].getConsumedPartitionId());
		assertTrue(desc[0].getConsumedPartitionLocation().isUnknown());
		assertNull(desc[0].getConsumedPartitionLocation().getConnectionId());


		try {
			// Fail if lazy deployment is *not* allowed
			allowLazyDeployment = false;

			InputChannelDeploymentDescriptor.fromEdges(
				new ExecutionEdge[]{unknownEdge},
				consumerSlot,
				allowLazyDeployment);

			fail("Did not throw expected IllegalStateException");
		} catch (IllegalStateException ignored) {
		}
	}

	// ------------------------------------------------------------------------

	private static InstanceConnectionInfo createConnInfo(int port) {
		return new InstanceConnectionInfo(InetAddress.getLoopbackAddress(), port);
	}

	private static SimpleSlot mockSlot(InstanceConnectionInfo connInfo) {
		SimpleSlot slot = mock(SimpleSlot.class);
		Instance instance = mock(Instance.class);
		when(slot.getInstance()).thenReturn(instance);
		when(instance.getInstanceConnectionInfo()).thenReturn(connInfo);

		return slot;
	}

	private static ExecutionVertex mockExecutionVertex(ExecutionState state, SimpleSlot slot) {
		ExecutionVertex vertex = mock(ExecutionVertex.class);

		Execution exec = mock(Execution.class);
		when(exec.getState()).thenReturn(state);
		when(exec.getAttemptId()).thenReturn(new ExecutionAttemptID());

		when(exec.getAssignedResource()).thenReturn(slot);
		when(vertex.getCurrentAssignedResource()).thenReturn(slot);

		when(vertex.getCurrentExecutionAttempt()).thenReturn(exec);

		return vertex;
	}

	private static IntermediateResultPartition mockPartition(ExecutionVertex producer) {
		IntermediateResultPartition partition = mock(IntermediateResultPartition.class);
		when(partition.isConsumable()).thenReturn(true);

		IntermediateResult result = mock(IntermediateResult.class);
		when(result.getConnectionIndex()).thenReturn(0);

		when(partition.getIntermediateResult()).thenReturn(result);
		when(partition.getPartitionId()).thenReturn(new IntermediateResultPartitionID());

		when(partition.getProducer()).thenReturn(producer);

		return partition;
	}
}
