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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple wrapper for the partition readerQueue iterator, which increments a
 * sequence number for each returned buffer and remembers the receiver ID.
 *
 * <p>It also keeps track of available buffers and notifies the outbound
 * handler about non-emptiness, similar to the {@link LocalInputChannel}.
 */
class SequenceNumberingViewReader implements BufferAvailabilityListener {

	private final Object requestLock = new Object();

	private final InputChannelID receiverId;

	private final AtomicLong numBuffersAvailable = new AtomicLong();

	private final PartitionRequestQueue requestQueue;

	private volatile ResultSubpartitionView subpartitionView;

	/**
	 * The status indicating whether this reader is already enqueued in the pipeline for transferring
	 * data or not.
	 *
	 * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
	 * thread only since there is no synchronisation.
	 */
	private boolean isRegisteredAsAvailable = false;

	/** The number of available buffers for holding data on the consumer side. */
	private int numCreditsAvailable;

	private int sequenceNumber = -1;

	SequenceNumberingViewReader(
		InputChannelID receiverId,
		int initialCredit,
		PartitionRequestQueue requestQueue) {

		this.receiverId = receiverId;
		this.numCreditsAvailable = initialCredit;
		this.requestQueue = requestQueue;
	}

	void requestSubpartitionView(
		ResultPartitionProvider partitionProvider,
		ResultPartitionID resultPartitionId,
		int subPartitionIndex) throws IOException {

		synchronized (requestLock) {
			if (subpartitionView == null) {
				// This this call can trigger a notification we have to
				// schedule a separate task at the event loop that will
				// start consuming this. Otherwise the reference to the
				// view cannot be available in getNextBuffer().
				this.subpartitionView = partitionProvider.createSubpartitionView(
					resultPartitionId,
					subPartitionIndex,
					this);
			} else {
				throw new IllegalStateException("Subpartition already requested");
			}
		}
	}

	/**
	 * The credits from consumer are added in incremental way.
	 *
	 * @param creditDeltas The credit deltas
	 */
	public void addCredit(int creditDeltas) {
		numCreditsAvailable += creditDeltas;
	}

	/**
	 * Updates the value to indicate whether the reader is enqueued in the pipeline or not.
	 *
	 * @param isRegisteredAvailable True if this reader is already enqueued in the pipeline.
	 */
	public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
		this.isRegisteredAsAvailable = isRegisteredAvailable;
	}

	public boolean isRegisteredAsAvailable() {
		return isRegisteredAsAvailable;
	}

	/**
	 * Check whether this reader is available or not.
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 */
	public boolean isAvailable() {
		// BEWARE: this must be in sync with #isAvailable()!
		return numBuffersAvailable.get() > 0 &&
			(numCreditsAvailable > 0 || subpartitionView.nextBufferIsEvent());
	}

	/**
	 * Check whether this reader is available or not (internal use, in sync with
	 * {@link #isAvailable()}, but slightly faster).
	 *
	 * <p>Returns true only if the next buffer is an event or the reader has both available
	 * credits and buffers.
	 *
	 * @param bufferAndBacklog
	 * 		current buffer and backlog including information about the next buffer
	 * @param remaining
	 * 		remaining number of queued buffers, i.e. <tt>numBuffersAvailable.get()</tt>
	 */
	private boolean isAvailable(BufferAndBacklog bufferAndBacklog, long remaining) {
		return remaining > 0 &&
			(numCreditsAvailable > 0 || bufferAndBacklog.nextBufferIsEvent());
	}

	InputChannelID getReceiverId() {
		return receiverId;
	}

	int getSequenceNumber() {
		return sequenceNumber;
	}

	@VisibleForTesting
	int getNumCreditsAvailable() {
		return numCreditsAvailable;
	}

	@VisibleForTesting
	long getNumBuffersAvailable() {
		return numBuffersAvailable.get();
	}

	public BufferAndAvailability getNextBuffer() throws IOException, InterruptedException {
		BufferAndBacklog next = subpartitionView.getNextBuffer();
		if (next != null) {
			long remaining = numBuffersAvailable.decrementAndGet();
			sequenceNumber++;

			if (remaining < 0) {
				throw new IllegalStateException("no buffer available");
			}

			if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
				throw new IllegalStateException("no credit available");
			}

			return new BufferAndAvailability(
				next.buffer(), isAvailable(next, remaining), next.buffersInBacklog());
		} else {
			return null;
		}
	}

	public void notifySubpartitionConsumed() throws IOException {
		subpartitionView.notifySubpartitionConsumed();
	}

	public boolean isReleased() {
		return subpartitionView.isReleased();
	}

	public Throwable getFailureCause() {
		return subpartitionView.getFailureCause();
	}

	public void releaseAllResources() throws IOException {
		subpartitionView.releaseAllResources();
	}

	@Override
	public void notifyBuffersAvailable(long numBuffers) {
		// if this request made the channel non-empty, notify the input gate
		if (numBuffers > 0 && numBuffersAvailable.getAndAdd(numBuffers) == 0) {
			requestQueue.notifyReaderNonEmpty(this);
		}
	}

	@Override
	public String toString() {
		return "SequenceNumberingViewReader{" +
			"requestLock=" + requestLock +
			", receiverId=" + receiverId +
			", numBuffersAvailable=" + numBuffersAvailable.get() +
			", sequenceNumber=" + sequenceNumber +
			", numCreditsAvailable=" + numCreditsAvailable +
			", isRegisteredAsAvailable=" + isRegisteredAsAvailable +
			'}';
	}
}
