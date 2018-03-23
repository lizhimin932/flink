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

package org.apache.flink.runtime.jobmaster.factories;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;

import javax.annotation.Nonnull;

/**
 * Implementation of {@link JobManagerJobMetricGroupFactory} which makes sure that there is always
 * at most one {@link JobManagerJobMetricGroup} for a given {@link JobID} registered.
 */
public class DefaultJobManagerJobMetricGroupFactory implements JobManagerJobMetricGroupFactory {

	private final JobManagerMetricGroup jobManagerMetricGroup;

	public DefaultJobManagerJobMetricGroupFactory(@Nonnull JobManagerMetricGroup jobManagerMetricGroup) {
		this.jobManagerMetricGroup = jobManagerMetricGroup;
	}

	@Override
	public JobManagerJobMetricGroup create(@Nonnull JobGraph jobGraph) {
		jobManagerMetricGroup.removeJob(jobGraph.getJobID());

		return jobManagerMetricGroup.addJob(jobGraph);
	}
}
