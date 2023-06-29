/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmanager.scheduler.Locality;
import org.apache.flink.runtime.jobmaster.SlotInfo;

import javax.annotation.Nonnull;

import java.util.Optional;
import java.util.function.Supplier;

class DefaultLocationPreferenceSlotSelectionStrategy
        extends LocationPreferenceSlotSelectionStrategy {

    @Nonnull
    @Override
    protected Optional<SlotInfoAndLocality> selectWithoutLocationPreference(
            @Nonnull FreeSlotInfoTracker freeSlotInfoTracker,
            @Nonnull ResourceProfile resourceProfile) {
        for (AllocationID allocationId : freeSlotInfoTracker.getAvailableSlots()) {
            SlotInfo candidate = freeSlotInfoTracker.getSlotInfo(allocationId);
            if (candidate.getResourceProfile().isMatching(resourceProfile)) {
                return Optional.of(SlotInfoAndLocality.of(candidate, Locality.UNCONSTRAINED));
            }
        }
        return Optional.empty();
    }

    @Override
    protected double calculateCandidateScore(
            int localWeigh, int hostLocalWeigh, Supplier<Double> taskExecutorUtilizationSupplier) {
        return localWeigh * 10 + hostLocalWeigh;
    }
}
