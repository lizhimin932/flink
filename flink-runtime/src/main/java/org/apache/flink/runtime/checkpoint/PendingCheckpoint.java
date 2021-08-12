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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A pending checkpoint is a checkpoint that has been started, but has not been acknowledged by all
 * tasks that need to acknowledge it. Once all tasks have acknowledged it, it becomes a {@link
 * CompletedCheckpoint}.
 *
 * <p>Note that the pending checkpoint, as well as the successful checkpoint keep the state handles
 * always as serialized values, never as actual values.
 */
public class PendingCheckpoint implements Checkpoint {

    /** Result of the {@link PendingCheckpoint#acknowledgedTasks} method. */
    public enum TaskAcknowledgeResult {
        SUCCESS, // successful acknowledge of the task
        DUPLICATE, // acknowledge message is a duplicate
        UNKNOWN, // unknown task acknowledged
        DISCARDED // pending checkpoint has been discarded
    }

    // ------------------------------------------------------------------------

    /** The PendingCheckpoint logs to the same logger as the CheckpointCoordinator. */
    private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

    private final Object lock = new Object();

    private final JobID jobId;

    private final long checkpointId;

    private final long checkpointTimestamp;

    private final Map<OperatorID, OperatorState> operatorStates;

    private final CheckpointPlan checkpointPlan;

    private final Map<ExecutionAttemptID, ExecutionVertex> notYetAcknowledgedTasks;

    private final Set<OperatorID> notYetAcknowledgedOperatorCoordinators;

    private final List<MasterState> masterStates;

    private final Set<String> notYetAcknowledgedMasterStates;

    /** Set of acknowledged tasks. */
    private final Set<ExecutionAttemptID> acknowledgedTasks;

    private final Set<JobVertexID> fullyFinishedOrFinishedOnRestoreVertex;

    private final IdentityHashMap<ExecutionJobVertex, Integer> vertexOperatorsFinishedTasksCount;

    /** The checkpoint properties. */
    private final CheckpointProperties props;

    /** Target storage location to persist the checkpoint metadata to. */
    private final CheckpointStorageLocation targetLocation;

    /** The promise to fulfill once the checkpoint has been completed. */
    private final CompletableFuture<CompletedCheckpoint> onCompletionPromise;

    private int numAcknowledgedTasks;

    private boolean disposed;

    private boolean discarded;

    private volatile ScheduledFuture<?> cancellerHandle;

    private CheckpointException failureCause;

    // --------------------------------------------------------------------------------------------

    public PendingCheckpoint(
            JobID jobId,
            long checkpointId,
            long checkpointTimestamp,
            CheckpointPlan checkpointPlan,
            Collection<OperatorID> operatorCoordinatorsToConfirm,
            Collection<String> masterStateIdentifiers,
            CheckpointProperties props,
            CheckpointStorageLocation targetLocation,
            CompletableFuture<CompletedCheckpoint> onCompletionPromise) {

        checkArgument(
                checkpointPlan.getTasksToWaitFor().size() > 0,
                "Checkpoint needs at least one vertex that commits the checkpoint");

        this.jobId = checkNotNull(jobId);
        this.checkpointId = checkpointId;
        this.checkpointTimestamp = checkpointTimestamp;
        this.checkpointPlan = checkNotNull(checkpointPlan);

        this.notYetAcknowledgedTasks = new HashMap<>(checkpointPlan.getTasksToWaitFor().size());
        for (Execution execution : checkpointPlan.getTasksToWaitFor()) {
            notYetAcknowledgedTasks.put(execution.getAttemptId(), execution.getVertex());
        }

        this.props = checkNotNull(props);
        this.targetLocation = checkNotNull(targetLocation);

        this.operatorStates = new HashMap<>();
        this.masterStates = new ArrayList<>(masterStateIdentifiers.size());
        this.notYetAcknowledgedMasterStates =
                masterStateIdentifiers.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(masterStateIdentifiers);
        this.notYetAcknowledgedOperatorCoordinators =
                operatorCoordinatorsToConfirm.isEmpty()
                        ? Collections.emptySet()
                        : new HashSet<>(operatorCoordinatorsToConfirm);
        this.acknowledgedTasks = new HashSet<>(checkpointPlan.getTasksToWaitFor().size());

        this.fullyFinishedOrFinishedOnRestoreVertex = new HashSet<>();
        checkpointPlan
                .getFullyFinishedJobVertex()
                .forEach(
                        jobVertex ->
                                fullyFinishedOrFinishedOnRestoreVertex.add(
                                        jobVertex.getJobVertexId()));
        this.vertexOperatorsFinishedTasksCount = new IdentityHashMap<>();

        this.onCompletionPromise = checkNotNull(onCompletionPromise);
    }

    // --------------------------------------------------------------------------------------------

    // ------------------------------------------------------------------------
    //  Properties
    // ------------------------------------------------------------------------

    public JobID getJobId() {
        return jobId;
    }

    /** @deprecated use {@link #getCheckpointID()} */
    @Deprecated
    public long getCheckpointId() {
        return getCheckpointID();
    }

    @Override
    public long getCheckpointID() {
        return checkpointId;
    }

    public CheckpointStorageLocation getCheckpointStorageLocation() {
        return targetLocation;
    }

    public long getCheckpointTimestamp() {
        return checkpointTimestamp;
    }

    public int getNumberOfNonAcknowledgedTasks() {
        return notYetAcknowledgedTasks.size();
    }

    public int getNumberOfNonAcknowledgedOperatorCoordinators() {
        return notYetAcknowledgedOperatorCoordinators.size();
    }

    public CheckpointPlan getCheckpointPlan() {
        return checkpointPlan;
    }

    public int getNumberOfAcknowledgedTasks() {
        return numAcknowledgedTasks;
    }

    public Map<OperatorID, OperatorState> getOperatorStates() {
        return operatorStates;
    }

    public List<MasterState> getMasterStates() {
        return masterStates;
    }

    public boolean isFullyAcknowledged() {
        return areTasksFullyAcknowledged()
                && areCoordinatorsFullyAcknowledged()
                && areMasterStatesFullyAcknowledged();
    }

    boolean areMasterStatesFullyAcknowledged() {
        return notYetAcknowledgedMasterStates.isEmpty() && !disposed;
    }

    boolean areCoordinatorsFullyAcknowledged() {
        return notYetAcknowledgedOperatorCoordinators.isEmpty() && !disposed;
    }

    boolean areTasksFullyAcknowledged() {
        return notYetAcknowledgedTasks.isEmpty() && !disposed;
    }

    public boolean isAcknowledgedBy(ExecutionAttemptID executionAttemptId) {
        return !notYetAcknowledgedTasks.containsKey(executionAttemptId);
    }

    public boolean isDisposed() {
        return disposed;
    }

    /**
     * Checks whether this checkpoint can be subsumed or whether it should always continue,
     * regardless of newer checkpoints in progress.
     *
     * @return True if the checkpoint can be subsumed, false otherwise.
     */
    public boolean canBeSubsumed() {
        // If the checkpoint is forced, it cannot be subsumed.
        return !props.isSavepoint();
    }

    CheckpointProperties getProps() {
        return props;
    }

    /**
     * Sets the handle for the canceller to this pending checkpoint. This method fails with an
     * exception if a handle has already been set.
     *
     * @return true, if the handle was set, false, if the checkpoint is already disposed;
     */
    public boolean setCancellerHandle(ScheduledFuture<?> cancellerHandle) {
        synchronized (lock) {
            if (this.cancellerHandle == null) {
                if (!disposed) {
                    this.cancellerHandle = cancellerHandle;
                    return true;
                } else {
                    return false;
                }
            } else {
                throw new IllegalStateException("A canceller handle was already set");
            }
        }
    }

    public CheckpointException getFailureCause() {
        return failureCause;
    }

    // ------------------------------------------------------------------------
    //  Progress and Completion
    // ------------------------------------------------------------------------

    /**
     * Returns the completion future.
     *
     * @return A future to the completed checkpoint
     */
    public CompletableFuture<CompletedCheckpoint> getCompletionFuture() {
        return onCompletionPromise;
    }

    public CompletedCheckpoint finalizeCheckpoint(
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            @Nullable PendingCheckpointStats statsCallback)
            throws IOException {

        synchronized (lock) {
            checkState(!isDisposed(), "checkpoint is discarded");
            checkState(
                    isFullyAcknowledged(),
                    "Pending checkpoint has not been fully acknowledged yet");

            // make sure we fulfill the promise with an exception if something fails
            try {
                if (checkpointPlan.isMayHaveFinishedTasks()) {
                    Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex = new HashMap<>();
                    for (Execution task : checkpointPlan.getFinishedTasks()) {
                        JobVertexID jobVertexId = task.getVertex().getJobvertexId();
                        if (!fullyFinishedOrFinishedOnRestoreVertex.contains(jobVertexId)) {
                            partlyFinishedVertex.put(jobVertexId, task.getVertex().getJobVertex());
                        }
                    }

                    checkNoPartlyFinishedVertexUsedUnionListState(partlyFinishedVertex);
                    checkNoPartlyOperatorsFinishedVertexUsedUnionListState(partlyFinishedVertex);
                }

                fulfillFullyFinishedOperatorStates();

                // write out the metadata
                final CheckpointMetadata savepoint =
                        new CheckpointMetadata(checkpointId, operatorStates.values(), masterStates);
                final CompletedCheckpointStorageLocation finalizedLocation;

                try (CheckpointMetadataOutputStream out =
                        targetLocation.createMetadataOutputStream()) {
                    Checkpoints.storeCheckpointMetadata(savepoint, out);
                    finalizedLocation = out.closeAndFinalizeCheckpoint();
                }

                CompletedCheckpoint completed =
                        new CompletedCheckpoint(
                                jobId,
                                checkpointId,
                                checkpointTimestamp,
                                System.currentTimeMillis(),
                                operatorStates,
                                masterStates,
                                props,
                                finalizedLocation);

                onCompletionPromise.complete(completed);

                // to prevent null-pointers from concurrent modification, copy reference onto stack
                if (statsCallback != null) {
                    LOG.trace(
                            "Checkpoint {} size: {}Kb, duration: {}ms",
                            checkpointId,
                            statsCallback.getStateSize() == 0
                                    ? 0
                                    : statsCallback.getStateSize() / 1024,
                            statsCallback.getEndToEndDuration());
                    // Finalize the statsCallback and give the completed checkpoint a
                    // callback for discards.
                    CompletedCheckpointStats.DiscardCallback discardCallback =
                            statsCallback.reportCompletedCheckpoint(
                                    finalizedLocation.getExternalPointer());
                    completed.setDiscardCallback(discardCallback);
                }

                // mark this pending checkpoint as disposed, but do NOT drop the state
                dispose(false, checkpointsCleaner, postCleanup, executor);

                return completed;
            } catch (Throwable t) {
                onCompletionPromise.completeExceptionally(t);
                ExceptionUtils.rethrowIOException(t);
                return null; // silence the compiler
            }
        }
    }

    /**
     * If a job vertex using {@code UnionListState} has part of tasks FINISHED where others are
     * still in RUNNING state, the checkpoint would be aborted since it might cause incomplete
     * {@code UnionListState}.
     */
    private void checkNoPartlyFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex) {
        for (ExecutionJobVertex vertex : partlyFinishedVertex.values()) {
            if (hasUsedUnionListState(vertex)) {
                throw new FlinkRuntimeException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks are FINISHED.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    /**
     * If a job vertex using {@code UnionListState} has all the tasks in RUNNING state, but part of
     * the tasks have reported that the operators are finished, the checkpoint would be aborted.
     * This is to force the fast tasks wait for the slow tasks so that their final checkpoints would
     * be the same one, otherwise if the fast tasks finished, the slow tasks would be blocked
     * forever since all the following checkpoints would be aborted.
     */
    private void checkNoPartlyOperatorsFinishedVertexUsedUnionListState(
            Map<JobVertexID, ExecutionJobVertex> partlyFinishedVertex) {
        for (Map.Entry<ExecutionJobVertex, Integer> entry :
                vertexOperatorsFinishedTasksCount.entrySet()) {
            ExecutionJobVertex vertex = entry.getKey();

            // If the vertex is partly finished, then it must not used UnionListState
            // due to it passed the previous check.
            if (partlyFinishedVertex.containsKey(vertex.getJobVertexId())) {
                continue;
            }

            if (entry.getValue() != vertex.getParallelism() && hasUsedUnionListState(vertex)) {
                throw new FlinkRuntimeException(
                        String.format(
                                "The vertex %s (id = %s) has used"
                                        + " UnionListState, but part of its tasks has called operators' finish method.",
                                vertex.getName(), vertex.getJobVertexId()));
            }
        }
    }

    private boolean hasUsedUnionListState(ExecutionJobVertex vertex) {
        for (OperatorIDPair operatorIDPair : vertex.getOperatorIDs()) {
            OperatorState operatorState =
                    operatorStates.get(operatorIDPair.getGeneratedOperatorID());
            if (operatorState == null) {
                continue;
            }

            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                boolean hasUnionListState =
                        Stream.concat(
                                        operatorSubtaskState.getManagedOperatorState().stream(),
                                        operatorSubtaskState.getRawOperatorState().stream())
                                .filter(Objects::nonNull)
                                .flatMap(
                                        operatorStateHandle ->
                                                operatorStateHandle.getStateNameToPartitionOffsets()
                                                        .values().stream())
                                .anyMatch(
                                        stateMetaInfo ->
                                                stateMetaInfo.getDistributionMode()
                                                        == OperatorStateHandle.Mode.UNION);

                if (hasUnionListState) {
                    return true;
                }
            }
        }

        return false;
    }

    private void fulfillFullyFinishedOperatorStates() {
        // Completes the operator state for the fully finished operators
        for (ExecutionJobVertex jobVertex : checkpointPlan.getFullyFinishedJobVertex()) {
            for (OperatorIDPair operatorID : jobVertex.getOperatorIDs()) {
                OperatorState operatorState =
                        operatorStates.get(operatorID.getGeneratedOperatorID());
                checkState(
                        operatorState == null,
                        "There should be no states reported for fully finished operators");

                operatorState =
                        new FullyFinishedOperatorState(
                                operatorID.getGeneratedOperatorID(),
                                jobVertex.getParallelism(),
                                jobVertex.getMaxParallelism());
                operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
            }
        }
    }

    /**
     * Acknowledges the task with the given execution attempt id and the given subtask state.
     *
     * @param executionAttemptId of the acknowledged task
     * @param operatorSubtaskStates of the acknowledged task
     * @param metrics Checkpoint metrics for the stats
     * @return TaskAcknowledgeResult of the operation
     */
    public TaskAcknowledgeResult acknowledgeTask(
            ExecutionAttemptID executionAttemptId,
            TaskStateSnapshot operatorSubtaskStates,
            CheckpointMetrics metrics,
            @Nullable PendingCheckpointStats statsCallback) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final ExecutionVertex vertex = notYetAcknowledgedTasks.remove(executionAttemptId);

            if (vertex == null) {
                if (acknowledgedTasks.contains(executionAttemptId)) {
                    return TaskAcknowledgeResult.DUPLICATE;
                } else {
                    return TaskAcknowledgeResult.UNKNOWN;
                }
            } else {
                acknowledgedTasks.add(executionAttemptId);
            }

            List<OperatorIDPair> operatorIDs = vertex.getJobVertex().getOperatorIDs();
            long ackTimestamp = System.currentTimeMillis();

            for (OperatorIDPair operatorID : operatorIDs) {
                if (operatorSubtaskStates != null && operatorSubtaskStates.isFinishedOnRestore()) {
                    updateFinishedOnRestoreOperatorState(vertex, operatorID);
                } else {
                    updateNonFinishedOnRestoreOperatorState(
                            vertex, operatorSubtaskStates, operatorID);
                }
            }

            ++numAcknowledgedTasks;

            // publish the checkpoint statistics
            // to prevent null-pointers from concurrent modification, copy reference onto stack
            if (statsCallback != null) {
                // Do this in millis because the web frontend works with them
                long alignmentDurationMillis = metrics.getAlignmentDurationNanos() / 1_000_000;
                long checkpointStartDelayMillis =
                        metrics.getCheckpointStartDelayNanos() / 1_000_000;

                SubtaskStateStats subtaskStateStats =
                        new SubtaskStateStats(
                                vertex.getParallelSubtaskIndex(),
                                ackTimestamp,
                                metrics.getTotalBytesPersisted(),
                                metrics.getSyncDurationMillis(),
                                metrics.getAsyncDurationMillis(),
                                metrics.getBytesProcessedDuringAlignment(),
                                metrics.getBytesPersistedDuringAlignment(),
                                alignmentDurationMillis,
                                checkpointStartDelayMillis,
                                metrics.getUnalignedCheckpoint(),
                                true);

                LOG.trace(
                        "Checkpoint {} stats for {}: size={}Kb, duration={}ms, sync part={}ms, async part={}ms",
                        checkpointId,
                        vertex.getTaskNameWithSubtaskIndex(),
                        subtaskStateStats.getStateSize() == 0
                                ? 0
                                : subtaskStateStats.getStateSize() / 1024,
                        subtaskStateStats.getEndToEndDuration(statsCallback.getTriggerTimestamp()),
                        subtaskStateStats.getSyncCheckpointDuration(),
                        subtaskStateStats.getAsyncCheckpointDuration());
                statsCallback.reportSubtaskStats(vertex.getJobvertexId(), subtaskStateStats);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    private void updateFinishedOnRestoreOperatorState(
            ExecutionVertex vertex, OperatorIDPair operatorID) {
        OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new FullyFinishedOperatorState(
                            operatorID.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
        } else {
            checkState(
                    operatorState.isFullyFinished(),
                    String.format(
                            "The task %s(vertex id = %s) received finished snapshot, "
                                    + "but the vertex has also received non-finished snapshots previously, "
                                    + "which is impossible.",
                            vertex.getTaskNameWithSubtaskIndex(), vertex.getJobvertexId()));
        }

        fullyFinishedOrFinishedOnRestoreVertex.add(vertex.getJobvertexId());
    }

    private void updateNonFinishedOnRestoreOperatorState(
            ExecutionVertex vertex,
            TaskStateSnapshot operatorSubtaskStates,
            OperatorIDPair operatorID) {
        OperatorState operatorState = operatorStates.get(operatorID.getGeneratedOperatorID());

        if (operatorState == null) {
            operatorState =
                    new OperatorState(
                            operatorID.getGeneratedOperatorID(),
                            vertex.getTotalNumberOfParallelSubtasks(),
                            vertex.getMaxParallelism());
            operatorStates.put(operatorID.getGeneratedOperatorID(), operatorState);
        }
        OperatorSubtaskState operatorSubtaskState =
                operatorSubtaskStates == null
                        ? null
                        : operatorSubtaskStates.getSubtaskStateByOperatorID(
                                operatorID.getGeneratedOperatorID());

        if (operatorSubtaskState != null) {
            operatorState.putState(vertex.getParallelSubtaskIndex(), operatorSubtaskState);
        }

        if (operatorSubtaskStates != null && operatorSubtaskStates.isOperatorsFinished()) {
            vertexOperatorsFinishedTasksCount.compute(
                    vertex.getJobVertex(), (k, v) -> v == null ? 1 : v + 1);
        }
    }

    public TaskAcknowledgeResult acknowledgeCoordinatorState(
            OperatorInfo coordinatorInfo, @Nullable ByteStreamStateHandle stateHandle) {

        synchronized (lock) {
            if (disposed) {
                return TaskAcknowledgeResult.DISCARDED;
            }

            final OperatorID operatorId = coordinatorInfo.operatorId();
            OperatorState operatorState = operatorStates.get(operatorId);

            // sanity check for better error reporting
            if (!notYetAcknowledgedOperatorCoordinators.remove(operatorId)) {
                return operatorState != null && operatorState.getCoordinatorState() != null
                        ? TaskAcknowledgeResult.DUPLICATE
                        : TaskAcknowledgeResult.UNKNOWN;
            }

            if (operatorState == null) {
                operatorState =
                        new OperatorState(
                                operatorId,
                                coordinatorInfo.currentParallelism(),
                                coordinatorInfo.maxParallelism());
                operatorStates.put(operatorId, operatorState);
            }
            if (stateHandle != null) {
                operatorState.setCoordinatorState(stateHandle);
            }

            return TaskAcknowledgeResult.SUCCESS;
        }
    }

    /**
     * Acknowledges a master state (state generated on the checkpoint coordinator) to the pending
     * checkpoint.
     *
     * @param identifier The identifier of the master state
     * @param state The state to acknowledge
     */
    public void acknowledgeMasterState(String identifier, @Nullable MasterState state) {

        synchronized (lock) {
            if (!disposed) {
                if (notYetAcknowledgedMasterStates.remove(identifier) && state != null) {
                    masterStates.add(state);
                }
            }
        }
    }

    // ------------------------------------------------------------------------
    //  Cancellation
    // ------------------------------------------------------------------------

    /** Aborts a checkpoint with reason and cause. */
    public void abort(
            CheckpointFailureReason reason,
            @Nullable Throwable cause,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor,
            PendingCheckpointStats statsCallback) {
        try {
            failureCause = new CheckpointException(reason, cause);
            onCompletionPromise.completeExceptionally(failureCause);
            reportFailedCheckpoint(failureCause, statsCallback);
            assertAbortSubsumedForced(reason);
        } finally {
            dispose(true, checkpointsCleaner, postCleanup, executor);
        }
    }

    private void assertAbortSubsumedForced(CheckpointFailureReason reason) {
        if (props.isSavepoint() && reason == CheckpointFailureReason.CHECKPOINT_SUBSUMED) {
            throw new IllegalStateException(
                    "Bug: savepoints must never be subsumed, "
                            + "the abort reason is : "
                            + reason.message());
        }
    }

    private void dispose(
            boolean releaseState,
            CheckpointsCleaner checkpointsCleaner,
            Runnable postCleanup,
            Executor executor) {

        synchronized (lock) {
            try {
                numAcknowledgedTasks = -1;
                checkpointsCleaner.cleanCheckpoint(this, releaseState, postCleanup, executor);
            } finally {
                disposed = true;
                notYetAcknowledgedTasks.clear();
                acknowledgedTasks.clear();
                cancelCanceller();
            }
        }
    }

    /**
     * Discard state. Must be called after {@link #dispose(boolean, CheckpointsCleaner, Runnable,
     * Executor) dispose}.
     */
    @Override
    public void discard() {
        synchronized (lock) {
            if (discarded) {
                Preconditions.checkState(
                        disposed, "Checkpoint should be disposed before being discarded");
                return;
            } else {
                discarded = true;
            }
        }
        // discard the private states.
        // unregistered shared states are still considered private at this point.
        try {
            StateUtil.bestEffortDiscardAllStateObjects(operatorStates.values());
            targetLocation.disposeOnFailure();
        } catch (Throwable t) {
            LOG.warn(
                    "Could not properly dispose the private states in the pending checkpoint {} of job {}.",
                    checkpointId,
                    jobId,
                    t);
        } finally {
            operatorStates.clear();
        }
    }

    private void cancelCanceller() {
        try {
            final ScheduledFuture<?> canceller = this.cancellerHandle;
            if (canceller != null) {
                canceller.cancel(false);
            }
        } catch (Exception e) {
            // this code should not throw exceptions
            LOG.warn("Error while cancelling checkpoint timeout task", e);
        }
    }

    /**
     * Reports a failed checkpoint with the given optional cause.
     *
     * @param cause The failure cause or <code>null</code>.
     */
    private void reportFailedCheckpoint(Exception cause, PendingCheckpointStats statsCallback) {
        // to prevent null-pointers from concurrent modification, copy reference onto stack
        if (statsCallback != null) {
            long failureTimestamp = System.currentTimeMillis();
            statsCallback.reportFailedCheckpoint(failureTimestamp, cause);
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    @Override
    public String toString() {
        return String.format(
                "Pending Checkpoint %d @ %d - confirmed=%d, pending=%d",
                checkpointId,
                checkpointTimestamp,
                getNumberOfAcknowledgedTasks(),
                getNumberOfNonAcknowledgedTasks());
    }
}
