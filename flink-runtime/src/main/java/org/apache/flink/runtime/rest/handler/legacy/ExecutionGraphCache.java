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

package org.apache.flink.runtime.rest.handler.legacy;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.util.Preconditions;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Cache for {@link ArchivedExecutionGraph} which are obtained from the Flink cluster. Every cache entry
 * has an associated time to live after which a new request will trigger the reloading of the
 * {@link ArchivedExecutionGraph} from the cluster.
 */
public class ExecutionGraphCache implements Closeable {

	private final Time timeout;

	private final Time timeToLive;

	private final ConcurrentHashMap<JobID, ExecutionGraphEntry> cachedExecutionGraphs;

	private volatile boolean running = true;

	private ScheduledExecutorService scheduledExecutorService;

	public ExecutionGraphCache(
			Time timeout,
			Time timeToLive) {
		this.timeout = checkNotNull(timeout);
		this.timeToLive = checkNotNull(timeToLive);

		cachedExecutionGraphs = new ConcurrentHashMap<>(4);
		startCacheCleaner();
	}

	private void startCacheCleaner() {
		long cleanupInterval = timeToLive.toMilliseconds() * 20L;
		scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
		scheduledExecutorService.scheduleAtFixedRate(
			this::cleanup,
			0,
			cleanupInterval,
			TimeUnit.MILLISECONDS);
	}

	@Override
	public void close() {
		running = false;

		scheduledExecutorService.shutdown();

		// clear all cached AccessExecutionGraphs
		cachedExecutionGraphs.clear();
	}

	/**
	 * Gets the number of cache entries.
	 */
	public int size() {
		return cachedExecutionGraphs.size();
	}

	/**
	 * Gets the {@link AccessExecutionGraph} for the given {@link JobID} and caches it. The
	 * {@link AccessExecutionGraph} will be requested again after the refresh interval has passed
	 * or if the graph could not be retrieved from the given gateway.
	 *
	 * @param jobId identifying the {@link ArchivedExecutionGraph} to get
	 * @param restfulGateway to request the {@link ArchivedExecutionGraph} from
	 * @return Future containing the requested {@link ArchivedExecutionGraph}
	 */
	public CompletableFuture<AccessExecutionGraph> getExecutionGraph(JobID jobId, RestfulGateway restfulGateway) {
		return getExecutionGraphInternal(jobId, restfulGateway).thenApply(Function.identity());
	}

	private CompletableFuture<ArchivedExecutionGraph> getExecutionGraphInternal(JobID jobId, RestfulGateway restfulGateway) {
		Preconditions.checkState(running, "ExecutionGraphCache is no longer running");

		while (true) {
			final ExecutionGraphEntry oldEntry = cachedExecutionGraphs.get(jobId);

			final long currentTime = System.currentTimeMillis();

			if (oldEntry != null && currentTime < oldEntry.getTTL()) {
				final CompletableFuture<ArchivedExecutionGraph> executionGraphFuture = oldEntry.getExecutionGraphFuture();
				if (!executionGraphFuture.isCompletedExceptionally()) {
					return executionGraphFuture;
				}
				// otherwise it must be completed exceptionally
			}

			final ExecutionGraphEntry newEntry = new ExecutionGraphEntry(currentTime + timeToLive.toMilliseconds());

			final boolean successfulUpdate;

			if (oldEntry == null) {
				successfulUpdate = cachedExecutionGraphs.putIfAbsent(jobId, newEntry) == null;
			} else {
				successfulUpdate = cachedExecutionGraphs.replace(jobId, oldEntry, newEntry);
				// cancel potentially outstanding futures
				oldEntry.getExecutionGraphFuture().cancel(false);
			}

			if (successfulUpdate) {
				final CompletableFuture<ArchivedExecutionGraph> executionGraphFuture = restfulGateway.requestJob(jobId, timeout);

				executionGraphFuture.whenComplete(
					(ArchivedExecutionGraph executionGraph, Throwable throwable) -> {
						if (throwable != null) {
							newEntry.getExecutionGraphFuture().completeExceptionally(throwable);

							// remove exceptionally completed entry because it doesn't help
							cachedExecutionGraphs.remove(jobId, newEntry);
						} else {
							newEntry.getExecutionGraphFuture().complete(executionGraph);
						}
					});

				if (!running) {
					// delete newly added entry in case of a concurrent stopping operation
					cachedExecutionGraphs.remove(jobId, newEntry);
				}

				return newEntry.getExecutionGraphFuture();
			}
		}
	}

	/**
	 * Perform the cleanup of out dated {@link ExecutionGraphEntry}.
	 */
	public void cleanup() {
		long currentTime = System.currentTimeMillis();

		// remove entries which have exceeded their time to live
		cachedExecutionGraphs.values().removeIf(
			(ExecutionGraphEntry entry) -> currentTime >= entry.getTTL());
	}

	/**
	 * Wrapper containing the current execution graph and it's time to live (TTL).
	 */
	private static final class ExecutionGraphEntry {
		private final long ttl;

		private final CompletableFuture<ArchivedExecutionGraph> executionGraphFuture;

		ExecutionGraphEntry(long ttl) {
			this.ttl = ttl;
			this.executionGraphFuture = new CompletableFuture<>();
		}

		public long getTTL() {
			return ttl;
		}

		public CompletableFuture<ArchivedExecutionGraph> getExecutionGraphFuture() {
			return executionGraphFuture;
		}
	}
}
