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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.messages.JobManagerMessages;

/**
 * This implementation uses {@link ActorGateway} to trigger the partition state check at the job
 * manager.
 */
public class ActorGatewayPartitionStateChecker implements PartitionStateChecker {

	private final ActorGateway jobManager;

	private final ActorGateway taskManager;

	public ActorGatewayPartitionStateChecker(ActorGateway jobManager, ActorGateway taskManager) {
		this.jobManager = jobManager;
		this.taskManager = taskManager;
	}

	@Override
	public void triggerPartitionStateCheck(
		JobID jobId,
		ExecutionAttemptID executionAttemptID,
		IntermediateDataSetID resultId,
		ResultPartitionID partitionId) {

		JobManagerMessages.RequestPartitionState msg = new JobManagerMessages.RequestPartitionState(
			jobId,
			partitionId,
			executionAttemptID,
			resultId);

		jobManager.tell(msg, taskManager);
	}
}
