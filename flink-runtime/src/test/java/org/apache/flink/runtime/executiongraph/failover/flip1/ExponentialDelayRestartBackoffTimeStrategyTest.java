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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.clock.ManualClock;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link ExponentialDelayRestartBackoffTimeStrategy}. */
public class ExponentialDelayRestartBackoffTimeStrategyTest extends TestLogger {

    private final Exception failure = new Exception();

    @Test
    public void testAlwaysRestart() throws Exception {
        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), 1L, 3L, 2.0, 4L, 0.25);

        for (int i = 0; i < 13; i++) {
            assertTrue(restartStrategy.canRestart());
            restartStrategy.notifyFailure(failure);
        }
    }

    @Test
    public void testInitialBackoff() throws Exception {
        long initialBackoffMS = 42L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), initialBackoffMS, 45L, 2.0, 8L, 0);

        assertThat(restartStrategy.getBackoffTime(), is(initialBackoffMS));
    }

    @Test
    public void testMaxBackoff() throws Exception {
        final long maxBackoffMS = 6L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), 1L, maxBackoffMS, 2.0, 8L, 0.25);

        for (int i = 0; i < 10; i++) {
            restartStrategy.notifyFailure(failure);
            assertThat(restartStrategy.getBackoffTime(), lessThanOrEqualTo(maxBackoffMS));
        }
    }

    @Test
    public void testResetBackoff() throws Exception {
        final long initialBackoffMS = 1L;
        final long resetBackoffThresholdMS = 8L;
        final ManualClock clock = new ManualClock();

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock, initialBackoffMS, 5L, 2.0, resetBackoffThresholdMS, 0.25);

        clock.advanceTime(
                resetBackoffThresholdMS + restartStrategy.getBackoffTime() - 1,
                TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertThat("Backoff should be increased", restartStrategy.getBackoffTime(), is(2L));

        clock.advanceTime(
                resetBackoffThresholdMS + restartStrategy.getBackoffTime(), TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertThat(
                "Backoff should be reset", restartStrategy.getBackoffTime(), is(initialBackoffMS));
    }

    @Test
    public void testBackoffMultiplier() throws Exception {
        long initialBackoffMS = 4L;
        double jitterFactor = 0;
        double backoffMultiplier = 2.3;
        long maxBackoffMS = 300L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(),
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        8L,
                        jitterFactor);

        restartStrategy.notifyFailure(failure);
        assertThat(restartStrategy.getBackoffTime(), is(9L)); // 4 * 2.3

        restartStrategy.notifyFailure(failure);
        assertThat(restartStrategy.getBackoffTime(), is(20L)); // 9 * 2.3
    }

    @Test
    public void testJitter() throws Exception {
        final long initialBackoffMS = 2L;
        final long maxBackoffMS = 7L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), initialBackoffMS, maxBackoffMS, 2.0, 1L, 0.25);

        restartStrategy.notifyFailure(failure);
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 3L, 4L, 5L);

        restartStrategy.notifyFailure(failure);
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 6L, 7L);

        restartStrategy.notifyFailure(failure);
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 6L, 7L);
    }

    @Test
    public void testJitterNoHigherThanMax() throws Exception {
        double jitterFactor = 1;
        long maxBackoffMS = 7L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), 1L, maxBackoffMS, 2.0, 8L, jitterFactor);

        assertCorrectRandomRange(restartStrategy::getBackoffTime, 0L, 1L, 2L);

        restartStrategy.notifyFailure(failure);
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 0L, 1L, 2L, 3L, 4L);

        restartStrategy.notifyFailure(failure);
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
    }

    @Test
    public void testMultipleSettings() throws Exception {
        ManualClock clock = new ManualClock();
        final long initialBackoffMS = 1L;
        final long maxBackoffMS = 9L;
        double backoffMultiplier = 2.0;
        final long resetBackoffThresholdMS = 8L;
        double jitterFactor = 0.25;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        resetBackoffThresholdMS,
                        jitterFactor);

        // ensure initial data
        assertTrue(restartStrategy.canRestart());
        assertThat(restartStrategy.getBackoffTime(), is(initialBackoffMS));

        // ensure backoff time is initial after the first failure
        clock.advanceTime(50, TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertThat(restartStrategy.getBackoffTime(), is(initialBackoffMS));

        // ensure backoff increases until threshold is reached
        clock.advanceTime(4, TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertThat(restartStrategy.getBackoffTime(), is(2L));

        clock.advanceTime(3, TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 3L, 4L, 5L);

        clock.advanceTime(7, TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertCorrectRandomRange(restartStrategy::getBackoffTime, 6L, 7L, 8L, 9L);

        // ensure backoff is reset after threshold is reached
        clock.advanceTime(resetBackoffThresholdMS + 9 + 1, TimeUnit.MILLISECONDS);
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertThat(restartStrategy.getBackoffTime(), is(1L));

        // ensure backoff still increases
        restartStrategy.notifyFailure(failure);
        assertTrue(restartStrategy.canRestart());
        assertThat(restartStrategy.getBackoffTime(), is(2L));
    }

    private void assertCorrectRandomRange(Callable<Long> numberGenerator, Long... expectedNumbers)
            throws Exception {
        Set<Long> generatedNumbers = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            long generatedNumber = numberGenerator.call();
            generatedNumbers.add(generatedNumber);
        }
        assertThat(generatedNumbers, is(equalTo(new HashSet<>(Arrays.asList(expectedNumbers)))));
    }
}
