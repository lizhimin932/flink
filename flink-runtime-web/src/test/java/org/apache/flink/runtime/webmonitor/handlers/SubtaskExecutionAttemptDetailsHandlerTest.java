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
package org.apache.flink.runtime.webmonitor.handlers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecution;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.webmonitor.history.ArchivedJson;
import org.apache.flink.runtime.webmonitor.history.Archiver;
import org.apache.flink.runtime.webmonitor.utils.ArchivedJobGenerationUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SubtaskExecutionAttemptDetailsHandlerTest {

	@Test
	public void testArchiver() throws Exception {
		Archiver archiver = new SubtaskExecutionAttemptDetailsHandler.SubtaskExecutionAttemptDetailsArchiver();
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();

		ArchivedJson[] archives = archiver.archiveJsonWithPath(originalJob);
		Assert.assertEquals(2, archives.length);

		ArchivedJson archive1 = archives[0];
		Assert.assertEquals(
			"/jobs/" + originalJob.getJobID() +
				"/vertices/" + originalTask.getJobVertexId() +
				"/subtasks/" + originalAttempt.getParallelSubtaskIndex(),
			archive1.getPath());
		compareAttemptDetails(originalAttempt, archive1.getJson());

		ArchivedJson archive2 = archives[1];
		Assert.assertEquals(
			"/jobs/" + originalJob.getJobID() +
				"/vertices/" + originalTask.getJobVertexId() +
				"/subtasks/" + originalAttempt.getParallelSubtaskIndex() +
				"/attempts/" + originalAttempt.getAttemptNumber(),
			archive2.getPath());
		compareAttemptDetails(originalAttempt, archive2.getJson());
	}

	@Test
	public void testGetPaths() {
		SubtaskExecutionAttemptDetailsHandler handler = new SubtaskExecutionAttemptDetailsHandler(null, null);
		String[] paths = handler.getPaths();
		Assert.assertEquals(1, paths.length);
		Assert.assertEquals("/jobs/:jobid/vertices/:vertexid/subtasks/:subtasknum/attempts/:attempt", paths[0]);
	}

	@Test
	public void testJsonGeneration() throws Exception {
		AccessExecutionGraph originalJob = ArchivedJobGenerationUtils.getTestJob();
		AccessExecutionJobVertex originalTask = ArchivedJobGenerationUtils.getTestTask();
		AccessExecution originalAttempt = ArchivedJobGenerationUtils.getTestAttempt();
		String json = SubtaskExecutionAttemptDetailsHandler.createAttemptDetailsJson(
			originalAttempt, originalJob.getJobID().toString(), originalTask.getJobVertexId().toString(), null);

		compareAttemptDetails(originalAttempt, json);
	}
	
	private static void compareAttemptDetails(AccessExecution originalAttempt, String json) throws IOException {
		JsonNode result = ArchivedJobGenerationUtils.mapper.readTree(json);

		Assert.assertEquals(originalAttempt.getParallelSubtaskIndex(), result.get("subtask").asInt());
		Assert.assertEquals(originalAttempt.getState().name(), result.get("status").asText());
		Assert.assertEquals(originalAttempt.getAttemptNumber(), result.get("attempt").asInt());
		Assert.assertEquals(originalAttempt.getAssignedResourceLocation().getHostname(), result.get("host").asText());
		long start = originalAttempt.getStateTimestamp(ExecutionState.DEPLOYING);
		Assert.assertEquals(start, result.get("start-time").asLong());
		long end = originalAttempt.getStateTimestamp(ExecutionState.FINISHED);
		Assert.assertEquals(end, result.get("end-time").asLong());
		Assert.assertEquals(end - start, result.get("duration").asLong());

		ArchivedJobGenerationUtils.compareIoMetrics(originalAttempt.getIOMetrics(), result.get("metrics"));
	}
}
