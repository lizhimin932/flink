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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

/**
 * A test that validates that the concurrency checks in the Kryo Serializer
 * are not hard coded to active.
 *
 * <p>The debug initialization in the KryoSerializer happens together with class
 * initialization (that makes it peak efficient), which is why this test needs to
 * run in a fresh JVM fork, and the JVM fork of this test should not be reused.
 *
 * <p><b>Important:</b> If you see this test fail and the initial settings are still
 * correct, check the assumptions above (on fresh JVM fork).
 */
public class KryoSerializerConcurrencyCheckInactiveITCase extends TestLogger {

	// this sets the debug initialization back to its default, even if
	// by default tests modify it (implicitly via assertion loading)
	static {
		KryoSerializerDebugInitHelper.setToDebug = KryoSerializerDebugInitHelper.INITIAL_SETTING;
	}

	/**
	 * This test checks that concurrent access is not detected by default, meaning that
	 * the thread concurrency checks are off by default.
	 */
	@Test
	public void testWithNoConcurrencyCheck() throws Exception {
		// this test will fail on DEBUG log level: If we run the test with DEBUG log level
		// the KryoSerializer.CONCURRENT_ACCESS_CHECK will be enabled, causing a failure here.
		assumeFalse(log.isDebugEnabled());
		boolean assertionError;
		try {
			new KryoSerializerConcurrencyTest().testConcurrentUseOfSerializer();
			assertionError = false;
		}
		catch (AssertionError e) {
			assertionError = true;
		}

		assertTrue("testConcurrentUseOfSerializer() should have failed if " +
				"concurrency checks are off by default", assertionError);
	}
}
