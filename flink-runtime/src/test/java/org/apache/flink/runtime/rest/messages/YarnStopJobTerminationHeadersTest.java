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

package org.apache.flink.runtime.rest.messages;

import org.apache.flink.runtime.rest.HttpMethodWrapper;
import org.apache.flink.util.TestLogger;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link YarnStopJobTerminationHeaders}. */
public class YarnStopJobTerminationHeadersTest extends TestLogger {

    // instance under test
    private static final YarnStopJobTerminationHeaders instance =
            YarnStopJobTerminationHeaders.getInstance();

    @Test
    public void testMethod() {
        assertThat(instance.getHttpMethod(), is(HttpMethodWrapper.GET));
    }

    @Test
    public void testURL() {
        assertThat(instance.getTargetRestEndpointURL(), endsWith("yarn-stop"));
    }
}
