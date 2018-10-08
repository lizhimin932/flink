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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.messages.Acknowledge;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Interface for components which have to perform actions on allocated slots.
 */
public interface AllocatedSlotActions {

	/**
	 * Releases the slot with the given {@link SlotRequestId}. If the slot belonged to a
	 * slot sharing group, then the corresponding {@link SlotSharingGroupId} has to be
	 * provided. Additionally, one can provide a cause for the slot release.
	 *
	 * @param slotRequestId identifying the slot to release
	 * @param slotSharingGroupId identifying the slot sharing group to which the slot belongs, null if none
	 * @param cause of the slot release, null if none
	 * @return Acknowledge after the slot has been released
	 */
	@Deprecated
	Acknowledge releaseSlot(
		SlotRequestId slotRequestId,
		@Nullable SlotSharingGroupId slotSharingGroupId,
		@Nullable Throwable cause);

	/**
	 * Releases the slot with the given {@link SlotRequestId}. Additionally, one can provide a cause for the slot release.
	 *
	 * @param slotRequestId identifying the slot to release
	 * @param cause of the slot release, null if none
	 * @return Acknowledge after the slot has been released
	 */
	@Nonnull
	Acknowledge releaseSlot(
		@Nonnull SlotRequestId slotRequestId,
		@Nullable Throwable cause);
}
