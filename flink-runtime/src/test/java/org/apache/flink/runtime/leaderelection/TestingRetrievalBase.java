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

package org.apache.flink.runtime.leaderelection;

import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Base class which provides some convenience functions for testing purposes of {@link
 * LeaderRetrievalListener} and {@link
 * org.apache.flink.runtime.leaderretrieval.LeaderRetrievalEventHandler}.
 */
public class TestingRetrievalBase {

    private final BlockingQueue<LeaderInformation> leaderEventQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<Throwable> errorQueue = new LinkedBlockingQueue<>();

    private LeaderInformation leader = LeaderInformation.empty();
    private String oldAddress;
    private Throwable error;

    public String waitForNewLeader() throws Exception {
        throwExceptionIfNotNull();

        CommonTestUtils.waitUntilCondition(
                () -> {
                    leader = leaderEventQueue.take();
                    return leader != null
                            && !leader.isEmpty()
                            && !leader.getLeaderAddress().equals(oldAddress);
                });

        oldAddress = leader.getLeaderAddress();

        return leader.getLeaderAddress();
    }

    public void waitForEmptyLeaderInformation() throws Exception {
        throwExceptionIfNotNull();

        CommonTestUtils.waitUntilCondition(
                () -> {
                    leader = leaderEventQueue.take();
                    return leader != null && leader.isEmpty();
                });

        oldAddress = null;
    }

    public void waitForError() throws Exception {
        error = errorQueue.take();
    }

    public void handleError(Throwable ex) {
        errorQueue.offer(ex);
    }

    public LeaderInformation getLeader() {
        return leader;
    }

    public String getAddress() {
        return leader.getLeaderAddress();
    }

    public UUID getLeaderSessionID() {
        return leader.getLeaderSessionID();
    }

    public void offerToLeaderQueue(LeaderInformation leaderInformation) {
        leaderEventQueue.offer(leaderInformation);
    }

    public int getLeaderEventQueueSize() {
        return leaderEventQueue.size();
    }

    /**
     * Please use {@link #waitForError} before get the error.
     *
     * @return the error has been handled.
     */
    @Nullable
    public Throwable getError() {
        return error == null ? errorQueue.poll() : error;
    }

    private void throwExceptionIfNotNull() throws Exception {
        if (error != null) {
            ExceptionUtils.rethrowException(error);
        }
    }
}
