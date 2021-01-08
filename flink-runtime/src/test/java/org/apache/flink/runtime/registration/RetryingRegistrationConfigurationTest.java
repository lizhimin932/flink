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

package org.apache.flink.runtime.registration;

import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for the {@link RetryingRegistrationConfiguration}. */
public class RetryingRegistrationConfigurationTest extends TestLogger {

    @Test
    public void testConfigurationParsing() {
        final Configuration configuration = new Configuration();

        final long initialRegistrationTimeout = 1L;
        final long maxRegistrationTimeout = 2L;
        final long refusedRegistrationDelay = 3L;
        final long errorRegistrationDelay = 4L;

        configuration.setLong(
                ClusterOptions.INITIAL_REGISTRATION_TIMEOUT, initialRegistrationTimeout);
        configuration.setLong(ClusterOptions.MAX_REGISTRATION_TIMEOUT, maxRegistrationTimeout);
        configuration.setLong(ClusterOptions.REFUSED_REGISTRATION_DELAY, refusedRegistrationDelay);
        configuration.setLong(ClusterOptions.ERROR_REGISTRATION_DELAY, errorRegistrationDelay);

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        assertThat(
                retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis(),
                is(initialRegistrationTimeout));
        assertThat(
                retryingRegistrationConfiguration.getMaxRegistrationTimeoutMillis(),
                is(maxRegistrationTimeout));
        assertThat(
                retryingRegistrationConfiguration.getRefusedDelayMillis(),
                is(refusedRegistrationDelay));
        assertThat(
                retryingRegistrationConfiguration.getErrorDelayMillis(),
                is(errorRegistrationDelay));
    }

    @Test
    public void testConfigurationWithDeprecatedOptions() {
        final Configuration configuration = new Configuration();

        final Duration refusedRegistrationBackoff = Duration.ofMinutes(42L);
        final Duration registrationMaxBackoff = Duration.ofSeconds(1L);
        final Duration initialRegistrationBackoff = Duration.ofHours(1337L);

        configuration.set(
                TaskManagerOptions.REFUSED_REGISTRATION_BACKOFF, refusedRegistrationBackoff);
        configuration.set(TaskManagerOptions.REGISTRATION_MAX_BACKOFF, registrationMaxBackoff);
        configuration.set(
                TaskManagerOptions.INITIAL_REGISTRATION_BACKOFF, initialRegistrationBackoff);

        final RetryingRegistrationConfiguration retryingRegistrationConfiguration =
                RetryingRegistrationConfiguration.fromConfiguration(configuration);

        assertThat(
                retryingRegistrationConfiguration.getInitialRegistrationTimeoutMillis(),
                is(ClusterOptions.INITIAL_REGISTRATION_TIMEOUT.defaultValue()));
        assertThat(
                retryingRegistrationConfiguration.getRefusedDelayMillis(),
                is(ClusterOptions.REFUSED_REGISTRATION_DELAY.defaultValue()));
        assertThat(
                retryingRegistrationConfiguration.getMaxRegistrationTimeoutMillis(),
                is(ClusterOptions.MAX_REGISTRATION_TIMEOUT.defaultValue()));
    }
}
