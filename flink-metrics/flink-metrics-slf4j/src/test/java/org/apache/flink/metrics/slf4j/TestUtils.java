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

package org.apache.flink.metrics.slf4j;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Test utilities.
 */
public class TestUtils {
	private static final Logger LOG = LoggerFactory.getLogger(TestUtils.class);

	private static TestAppender testAppender;

	public static void addTestAppenderForRootLogger() {
		org.apache.log4j.Logger.getRootLogger().removeAllAppenders();

		org.apache.log4j.Logger logger = org.apache.log4j.LogManager.getLogger(Slf4jReporter.class);
		logger.setLevel(org.apache.log4j.Level.INFO);

		testAppender = new TestAppender();
		logger.addAppender(testAppender);
	}

	public static void checkForLogString(String expected) {
		LoggingEvent found = getEventContainingString(expected);
		if (found != null) {
			LOG.info("Found expected string '" + expected + "' in log message " + found);
			return;
		}
		Assert.fail("Unable to find expected string '" + expected + "' in log messages");
	}

	public static LoggingEvent getEventContainingString(String expected) {
		if (testAppender == null) {
			throw new NullPointerException("Initialize test appender first");
		}
		LoggingEvent found = null;
		// make sure that different threads are not logging while the logs are checked
		synchronized (testAppender.events) {
			for (LoggingEvent event : testAppender.events) {
				if (event.getMessage().toString().contains(expected)) {
					found = event;
					break;
				}
			}
		}
		return found;
	}

	private static class TestAppender extends AppenderSkeleton {
		public final List<LoggingEvent> events = new ArrayList<>();

		public void close() {
		}

		public boolean requiresLayout() {
			return false;
		}

		@Override
		protected void append(LoggingEvent event) {
			synchronized (events) {
				events.add(event);
			}
		}
	}
}
