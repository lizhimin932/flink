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
 * limitations under the License
 */

package org.apache.flink.runtime.scheduler.benchmark.e2e;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.LocationPreferenceSlotSelectionStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProvider;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlotProviderImpl;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPool;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.benchmark.JobConfiguration;
import org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkBase;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createDefaultJobVertices;
import static org.apache.flink.runtime.scheduler.benchmark.SchedulerBenchmarkUtils.createJobGraph;

/**
 * The base class of benchmarks related to {@link DefaultScheduler}'s creation, scheduling and
 * deploying.
 */
public class SchedulerEndToEndBenchmarkBase extends SchedulerBenchmarkBase {

    ComponentMainThreadExecutor mainThreadExecutor;

    JobGraph jobGraph;
    PhysicalSlotProvider physicalSlotProvider;
    SlotPool slotPool;

    public void setup(JobConfiguration jobConfiguration) throws Exception {
        super.setup();

        mainThreadExecutor =
                ComponentMainThreadExecutorServiceAdapter.forSingleThreadExecutor(
                        scheduledExecutorService);

        final List<JobVertex> jobVertices = createDefaultJobVertices(jobConfiguration);
        jobGraph = createJobGraph(jobVertices, jobConfiguration);

        slotPool = new DeclarativeSlotPoolBridgeBuilder().buildAndStart(mainThreadExecutor);
        physicalSlotProvider = createPhysicalSlotProvider(slotPool);
    }

    private static PhysicalSlotProvider createPhysicalSlotProvider(SlotPool slotPool) {

        return new PhysicalSlotProviderImpl(
                LocationPreferenceSlotSelectionStrategy.createDefault(), slotPool);
    }

    static DefaultScheduler createScheduler(
            JobGraph jobGraph,
            PhysicalSlotProvider physicalSlotProvider,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService executorService)
            throws Exception {
        return new SchedulerTestingUtils.DefaultSchedulerBuilder(
                        jobGraph, mainThreadExecutor, executorService)
                .setExecutionSlotAllocatorFactory(
                        SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory(
                                physicalSlotProvider))
                .build();
    }
}
