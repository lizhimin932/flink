/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.assigners.WindowStagger;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link WindowStagger}.
 */
public class WindowStaggerTest {
	private final long sizeInMilliseconds = 5000;

	@Test
	public void testWindowStagger() {
		assertEquals(0L, WindowStagger.ALIGNED.getStaggerOffset(500L, sizeInMilliseconds));
		assertEquals(500L, WindowStagger.NATURAL.getStaggerOffset(5500L, sizeInMilliseconds));
		assertTrue(0 < WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds));
		assertTrue(sizeInMilliseconds > WindowStagger.RANDOM.getStaggerOffset(0L, sizeInMilliseconds));
	}
}
