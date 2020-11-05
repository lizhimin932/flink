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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Controller that can alternate between aligned and unaligned checkpoints.
 */
@Internal
public class AlternatingController implements CheckpointBarrierBehaviourController {
	private final AlignedController alignedController;
	private final UnalignedController unalignedController;

	private CheckpointBarrierBehaviourController activeController;
	private long timeOutedBarrierId = -1; // used to shortcut timeout check

	public AlternatingController(
			AlignedController alignedController,
			UnalignedController unalignedController) {
		this.activeController = this.alignedController = alignedController;
		this.unalignedController = unalignedController;
	}

	@Override
	public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
		activeController = chooseController(barrier);
	}

	@Override
	public void barrierAnnouncement(
			InputChannelInfo channelInfo,
			CheckpointBarrier announcedBarrier,
			int sequenceNumber) throws IOException {

		Optional<CheckpointBarrier> maybeTimedOut = maybeTimeOut(announcedBarrier);
		announcedBarrier = maybeTimedOut.orElse(announcedBarrier);

		if (maybeTimedOut.isPresent() && activeController != unalignedController) {
			// Let's timeout this barrier
			unalignedController.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);
		}
		else {
			// Either we have already timed out before, or we are still going with aligned checkpoints
			activeController.barrierAnnouncement(channelInfo, announcedBarrier, sequenceNumber);
		}
	}

	@Override
	public Optional<CheckpointBarrier> barrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
		Optional<CheckpointBarrier> maybeTimedOut = maybeTimeOut(barrier);
		barrier = maybeTimedOut.orElse(barrier);

		checkState(!activeController.barrierReceived(channelInfo, barrier).isPresent());

		if (maybeTimedOut.isPresent()) {
			if (activeController == alignedController) {
				timeoutToUnalignedCheckpoint(channelInfo, barrier);
				return maybeTimedOut;
			}
			else {
				// TODO: add unit test for this
				alignedController.resumeConsumption(channelInfo);
			}
		}
		return Optional.empty();
	}

	@Override
	public Optional<CheckpointBarrier> preProcessFirstBarrier(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) throws IOException {
		return activeController.preProcessFirstBarrier(channelInfo, barrier);
	}

	private void timeoutToUnalignedCheckpoint(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) throws IOException {
		checkState(alignedController == activeController);

		// timeout all not yet processed barriers for which alignedController has processed an announcement
		for (Map.Entry<InputChannelInfo, Integer> entry : alignedController.getSequenceNumberInAnnouncedChannels().entrySet()) {
			InputChannelInfo unProcessedChannelInfo = entry.getKey();
			int announcedBarrierSequenceNumber = entry.getValue();
			unalignedController.barrierAnnouncement(unProcessedChannelInfo, barrier, announcedBarrierSequenceNumber);
		}

		// get blocked channels before resuming consumption
		Collection<InputChannelInfo> blockedChannels = alignedController.getBlockedChannels();
		alignedController.resumeConsumption();
		activeController = unalignedController;

		// alignedController might has already processed some barriers, so "migrate"/forward those calls to unalignedController.
		unalignedController.preProcessFirstBarrier(channelInfo, barrier);
		for (InputChannelInfo blockedChannel : blockedChannels) {
			unalignedController.barrierReceived(blockedChannel, barrier);
		}
	}

	@Override
	public Optional<CheckpointBarrier> postProcessLastBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
		// regardless of timeout or not, complete this checkpoint as it was started
		return activeController.postProcessLastBarrier(channelInfo, maybeTimeOut(barrier).orElse(barrier));
	}

	@Override
	public void abortPendingCheckpoint(long cancelledId, CheckpointException exception) throws IOException {
		activeController.abortPendingCheckpoint(cancelledId, exception);
	}

	@Override
	public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
		chooseController(barrier).obsoleteBarrierReceived(channelInfo, barrier);
	}

	private void checkActiveController(CheckpointBarrier barrier) {
		if (isAligned(barrier)) {
			checkState(activeController == alignedController);
		}
		else {
			checkState(activeController == unalignedController);
		}
	}

	private boolean isAligned(CheckpointBarrier barrier) {
		return barrier.getCheckpointOptions().needsAlignment();
	}

	private CheckpointBarrierBehaviourController chooseController(CheckpointBarrier barrier) {
		return isAligned(barrier) ? alignedController : unalignedController;
	}

	private Optional<CheckpointBarrier> maybeTimeOut(CheckpointBarrier barrier) {
		CheckpointOptions options = barrier.getCheckpointOptions();
		boolean shouldTimeout = (options.isTimeoutable()) && (
			barrier.getId() == timeOutedBarrierId ||
				(System.currentTimeMillis() - barrier.getTimestamp()) > options.getAlignmentTimeout());
		if (options.isUnalignedCheckpoint() || !shouldTimeout) {
			return Optional.empty();
		}
		else {
			timeOutedBarrierId = Math.max(timeOutedBarrierId, barrier.getId());
			return Optional.of(new CheckpointBarrier(
				barrier.getId(),
				barrier.getTimestamp(),
				options.toTimeouted()));
		}
	}
}
