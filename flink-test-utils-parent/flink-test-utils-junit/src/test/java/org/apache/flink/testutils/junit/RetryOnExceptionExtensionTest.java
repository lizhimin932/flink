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

package org.apache.flink.testutils.junit;

import org.apache.flink.testutils.junit.extensions.retry.RetryExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for the RetryOnException annotation. */
@ExtendWith(RetryExtension.class)
public class RetryOnExceptionExtensionTest {

    private static final int NUMBER_OF_RUNS = 3;

    private static int runsForSuccessfulTest = 0;

    private static int runsForTestWithMatchingException = 0;

    private static int runsForTestWithSubclassException = 0;

    private static int runsForPassAfterOneFailure = 0;

    @AfterAll
    public static void verify() {
        assertEquals(NUMBER_OF_RUNS + 1, runsForTestWithMatchingException);
        assertEquals(NUMBER_OF_RUNS + 1, runsForTestWithSubclassException);
        assertEquals(1, runsForSuccessfulTest);
        assertEquals(2, runsForPassAfterOneFailure);
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    public void testSuccessfulTest() {
        runsForSuccessfulTest++;
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    public void testMatchingException() {
        runsForTestWithMatchingException++;
        if (runsForTestWithMatchingException <= NUMBER_OF_RUNS) {
            throw new IllegalArgumentException();
        }
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = RuntimeException.class)
    public void testSubclassException() {
        runsForTestWithSubclassException++;
        if (runsForTestWithSubclassException <= NUMBER_OF_RUNS) {
            throw new IllegalArgumentException();
        }
    }

    @TestTemplate
    @RetryOnException(times = NUMBER_OF_RUNS, exception = IllegalArgumentException.class)
    public void testPassAfterOneFailure() {
        runsForPassAfterOneFailure++;
        if (runsForPassAfterOneFailure == 1) {
            throw new IllegalArgumentException();
        }
    }
}
