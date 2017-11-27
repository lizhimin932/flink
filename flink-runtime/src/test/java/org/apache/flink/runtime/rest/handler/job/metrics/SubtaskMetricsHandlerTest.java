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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.dump.QueryScopeInfo;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Tests for {@link SubtaskMetricsHandler}.
 */
public class SubtaskMetricsHandlerTest extends MetricsHandlerTestBase<SubtaskMetricsHandler> {

	private static final String TEST_JOB_ID = new JobID().toString();

	private static final String TEST_VERTEX_ID = new JobVertexID().toString();

	private static final int TEST_SUBTASK_INDEX = 0;

	@Override
	SubtaskMetricsHandler getMetricsHandler() {
		return new SubtaskMetricsHandler(
			CompletableFuture.completedFuture("localhost:12345"),
			leaderRetriever,
			TIMEOUT,
			TEST_HEADERS,
			mockMetricFetcher
		);
	}

	@Override
	QueryScopeInfo getQueryScopeInfo() {
		return new QueryScopeInfo.TaskQueryScopeInfo(TEST_JOB_ID, TEST_VERTEX_ID,
			TEST_SUBTASK_INDEX);
	}

	@Override
	Map<String, String> getPathParameters() {
		final Map<String, String> pathParameters = new HashMap<>();
		pathParameters.put("jobid", TEST_JOB_ID);
		pathParameters.put("vertexid", TEST_VERTEX_ID);
		pathParameters.put("subtaskindex", Integer.toString(TEST_SUBTASK_INDEX));
		return pathParameters;
	}

}
