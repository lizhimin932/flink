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

package org.apache.flink.runtime.rest.handler.job;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobVertexMessageParameters;
import org.apache.flink.runtime.rest.messages.MessageHeaders;
import org.apache.flink.runtime.rest.messages.job.SubtasksAllAccumulatorsInfo;
import org.apache.flink.runtime.rest.messages.job.UserAccumulator;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Request handler for the subtasks all accumulators.
 */
public class SubtasksAllAccumulatorsHandler extends AbstractJobVertexHandler<SubtasksAllAccumulatorsInfo, JobVertexMessageParameters> {

	/**
	 * Instantiates a new subtasks all accumulators handler.
	 *
	 * @param localRestAddress    the local rest address
	 * @param leaderRetriever     the leader retriever
	 * @param timeout             the timeout
	 * @param responseHeaders     the response headers
	 * @param messageHeaders      the message headers
	 * @param executionGraphCache the execution graph cache
	 * @param executor            the executor
	 */
	public SubtasksAllAccumulatorsHandler(CompletableFuture<String> localRestAddress, GatewayRetriever<? extends RestfulGateway> leaderRetriever, Time timeout, Map<String, String> responseHeaders, MessageHeaders<EmptyRequestBody, SubtasksAllAccumulatorsInfo, JobVertexMessageParameters> messageHeaders, ExecutionGraphCache executionGraphCache, Executor executor) {
		super(localRestAddress, leaderRetriever, timeout, responseHeaders, messageHeaders, executionGraphCache, executor);
	}

	@Override
	protected SubtasksAllAccumulatorsInfo handleRequest(HandlerRequest<EmptyRequestBody, JobVertexMessageParameters> request, AccessExecutionJobVertex jobVertex) throws RestHandlerException {
		String vertexId = jobVertex.getJobVertexId().toString();
		int parallelism = jobVertex.getParallelism();

		final List<SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo> subtaskAccumulatorsInfos = new ArrayList<>();

		int index = 0;
		for (AccessExecutionVertex vertex : jobVertex.getTaskVertices()) {
			TaskManagerLocation location = vertex.getCurrentAssignedResourceLocation();
			String locationString = location == null ? "(unassigned)" : location.getHostname();

			StringifiedAccumulatorResult[] accs = vertex.getCurrentExecutionAttempt().getUserAccumulatorsStringified();
			List<UserAccumulator> userAccumulators = new ArrayList<>(accs.length);
			for (StringifiedAccumulatorResult acc : accs) {
				userAccumulators.add(new UserAccumulator(acc.getName(), acc.getType(), acc.getValue()));
			}

			subtaskAccumulatorsInfos.add(
				new SubtasksAllAccumulatorsInfo.SubtaskAccumulatorsInfo(
					index++,
					vertex.getCurrentExecutionAttempt().getAttemptNumber(),
					locationString,
					userAccumulators
				));
		}

		return new SubtasksAllAccumulatorsInfo(vertexId, parallelism, subtaskAccumulatorsInfos);
	}

}
