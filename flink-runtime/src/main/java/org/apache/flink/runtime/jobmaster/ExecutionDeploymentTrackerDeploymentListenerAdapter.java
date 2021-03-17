/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;

/**
 * An adapter for using an {@link ExecutionDeploymentTracker} as an {@link
 * ExecutionDeploymentListener}.
 */
public class ExecutionDeploymentTrackerDeploymentListenerAdapter
        implements ExecutionDeploymentListener {
    private final ExecutionDeploymentTracker executionDeploymentTracker;

    public ExecutionDeploymentTrackerDeploymentListenerAdapter(
            ExecutionDeploymentTracker executionDeploymentTracker) {
        this.executionDeploymentTracker = executionDeploymentTracker;
    }

    @Override
    public void onStartedDeployment(ExecutionAttemptID execution, ResourceID host) {
        executionDeploymentTracker.startTrackingPendingDeploymentOf(execution, host);
    }

    @Override
    public void onCompletedDeployment(ExecutionAttemptID execution) {
        executionDeploymentTracker.completeDeploymentOf(execution);
    }
}
