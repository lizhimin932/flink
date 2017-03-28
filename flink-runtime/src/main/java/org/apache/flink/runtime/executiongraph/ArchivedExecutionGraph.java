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
package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.JobSnapshottingSettings;
import org.apache.flink.runtime.util.EvictingBoundedList;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class ArchivedExecutionGraph implements AccessExecutionGraph, Serializable {

	private static final long serialVersionUID = 7231383912742578428L;
	// --------------------------------------------------------------------------------------------

	/** The ID of the job this graph has been built for. */
	private final JobID jobID;

	/** The name of the original job graph. */
	private final String jobName;

	/** All job vertices that are part of this graph */
	private final Map<JobVertexID, ArchivedExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created **/
	private final List<ArchivedExecutionJobVertex> verticesInCreationOrder;

	/**
	 * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}.
	 */
	private final long[] stateTimestamps;

	// ------ Configuration of the Execution -------

	// ------ Execution status and progress. These values are volatile, and accessed under the lock -------

	/** Current status of the job execution */
	private final JobStatus state;

	/**
	 * The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure
	 */
	private final ErrorInfo failureCause;

	private EvictingBoundedList<ErrorInfo> priorFailureCauses;
	// ------ Fields that are only relevant for archived execution graphs ------------
	private final String jsonPlan;
	private final StringifiedAccumulatorResult[] archivedUserAccumulators;
	private final ArchivedExecutionConfig archivedExecutionConfig;
	private final boolean isStoppable;
	private final Map<String, SerializedValue<Object>> serializedUserAccumulators;

	@Nullable
	private final JobSnapshottingSettings jobSnapshottingSettings;

	@Nullable
	private final CheckpointStatsSnapshot checkpointStatsSnapshot;

	public ArchivedExecutionGraph(
			JobID jobID,
			String jobName,
			Map<JobVertexID, ArchivedExecutionJobVertex> tasks,
			List<ArchivedExecutionJobVertex> verticesInCreationOrder,
			long[] stateTimestamps,
			JobStatus state,
			ErrorInfo failureCause,
			EvictingBoundedList<ErrorInfo> priorFailureCauses,
			String jsonPlan,
			StringifiedAccumulatorResult[] archivedUserAccumulators,
			Map<String, SerializedValue<Object>> serializedUserAccumulators,
			ArchivedExecutionConfig executionConfig,
			boolean isStoppable,
			@Nullable JobSnapshottingSettings jobSnapshottingSettings,
			@Nullable CheckpointStatsSnapshot checkpointStatsSnapshot) {

		this.jobID = jobID;
		this.jobName = jobName;
		this.tasks = tasks;
		this.verticesInCreationOrder = verticesInCreationOrder;
		this.stateTimestamps = stateTimestamps;
		this.state = state;
		this.failureCause = failureCause;
		this.priorFailureCauses = priorFailureCauses;
		this.jsonPlan = jsonPlan;
		this.archivedUserAccumulators = archivedUserAccumulators;
		this.serializedUserAccumulators = serializedUserAccumulators;
		this.archivedExecutionConfig = executionConfig;
		this.isStoppable = isStoppable;
		this.jobSnapshottingSettings = jobSnapshottingSettings;
		this.checkpointStatsSnapshot = checkpointStatsSnapshot;
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public String getJsonPlan() {
		return jsonPlan;
	}

	@Override
	public JobID getJobID() {
		return jobID;
	}

	@Override
	public String getJobName() {
		return jobName;
	}

	@Override
	public JobStatus getState() {
		return state;
	}

	@Override
	public ErrorInfo getFailureCause() {
		return failureCause;
	}

	@Override
	public EvictingBoundedList<ErrorInfo> getPriorFailureCauses() {
		return priorFailureCauses;
	}

	@Override
	public ArchivedExecutionJobVertex getJobVertex(JobVertexID id) {
		return this.tasks.get(id);
	}

	@Override
	public Map<JobVertexID, AccessExecutionJobVertex> getAllVertices() {
		return Collections.<JobVertexID, AccessExecutionJobVertex>unmodifiableMap(this.tasks);
	}

	@Override
	public Iterable<ArchivedExecutionJobVertex> getVerticesTopologically() {
		// we return a specific iterator that does not fail with concurrent modifications
		// the list is append only, so it is safe for that
		final int numElements = this.verticesInCreationOrder.size();

		return new Iterable<ArchivedExecutionJobVertex>() {
			@Override
			public Iterator<ArchivedExecutionJobVertex> iterator() {
				return new Iterator<ArchivedExecutionJobVertex>() {
					private int pos = 0;

					@Override
					public boolean hasNext() {
						return pos < numElements;
					}

					@Override
					public ArchivedExecutionJobVertex next() {
						if (hasNext()) {
							return verticesInCreationOrder.get(pos++);
						} else {
							throw new NoSuchElementException();
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	@Override
	public Iterable<ArchivedExecutionVertex> getAllExecutionVertices() {
		return new Iterable<ArchivedExecutionVertex>() {
			@Override
			public Iterator<ArchivedExecutionVertex> iterator() {
				return new AllVerticesIterator(getVerticesTopologically().iterator());
			}
		};
	}

	@Override
	public long getStatusTimestamp(JobStatus status) {
		return this.stateTimestamps[status.ordinal()];
	}

	@Override
	public CheckpointCoordinator getCheckpointCoordinator() {
		return null;
	}

	@Override
	public JobSnapshottingSettings getJobSnapshottingSettings() {
		return jobSnapshottingSettings;
	}

	@Override
	public CheckpointStatsSnapshot getCheckpointStatsSnapshot() {
		return checkpointStatsSnapshot;
	}

	@Override
	public boolean isArchived() {
		return true;
	}

	public StringifiedAccumulatorResult[] getUserAccumulators() {
		return archivedUserAccumulators;
	}

	public ArchivedExecutionConfig getArchivedExecutionConfig() {
		return archivedExecutionConfig;
	}

	@Override
	public boolean isStoppable() {
		return isStoppable;
	}

	@Override
	public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
		return archivedUserAccumulators;
	}

	@Override
	public Map<String, SerializedValue<Object>> getAccumulatorsSerialized() {
		return serializedUserAccumulators;
	}

	class AllVerticesIterator implements Iterator<ArchivedExecutionVertex> {

		private final Iterator<ArchivedExecutionJobVertex> jobVertices;

		private ArchivedExecutionVertex[] currVertices;

		private int currPos;


		public AllVerticesIterator(Iterator<ArchivedExecutionJobVertex> jobVertices) {
			this.jobVertices = jobVertices;
		}


		@Override
		public boolean hasNext() {
			while (true) {
				if (currVertices != null) {
					if (currPos < currVertices.length) {
						return true;
					} else {
						currVertices = null;
					}
				} else if (jobVertices.hasNext()) {
					currVertices = jobVertices.next().getTaskVertices();
					currPos = 0;
				} else {
					return false;
				}
			}
		}

		@Override
		public ArchivedExecutionVertex next() {
			if (hasNext()) {
				return currVertices[currPos++];
			} else {
				throw new NoSuchElementException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
}
