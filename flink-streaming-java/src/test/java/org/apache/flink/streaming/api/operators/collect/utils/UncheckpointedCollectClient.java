/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.collect.utils;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.collect.CollectCoordinationResponse;

import org.junit.Assert;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.function.BooleanSupplier;

/**
 * A simple client for fetching collect results in checkpointed scenario.
 */
public class UncheckpointedCollectClient<T> implements TestCollectClient<T> {

	private static final String INIT_VERSION = "";

	private final TypeSerializer<T> serializer;
	private final CollectRequestSender<T> sender;
	private final BooleanSupplier jobFinishedChecker;

	private final List<T> results;

	public UncheckpointedCollectClient(
			TypeSerializer<T> serializer,
			CollectRequestSender<T> sender,
			BooleanSupplier jobFinishedChecker) {
		this.serializer = serializer;
		this.sender = sender;
		this.jobFinishedChecker = jobFinishedChecker;

		this.results = new LinkedList<>();
	}

	@Override
	public void run() {
		Random random = new Random();

		String version = INIT_VERSION;
		long offset = 0;

		try {
			while (!jobFinishedChecker.getAsBoolean()) {

				if (random.nextBoolean()) {
					Thread.sleep(random.nextInt(100));
				}

				CollectCoordinationResponse<T> response = sender.sendRequest(version, offset);
				String responseVersion = response.getVersion();
				long responseOffset = response.getOffset();
				List<T> responseResults = response.getResults(serializer);

				if (INIT_VERSION.equals(version)) {
					// first response, update version accordingly
					version = responseVersion;
				} else {
					if (version.equals(responseVersion)) {
						// normal results
						if (responseResults.size() > 0) {
							Assert.assertEquals(offset, responseOffset);
							results.addAll(responseResults);
							offset += responseResults.size();
						}
					} else {
						throw new RuntimeException("Sink restarted");
					}
				}
			}

			Tuple2<List<T>, Long> accResults = sender.getAccumulatorResults();
			results.addAll(accResults.f0.subList((int) (offset - accResults.f1), accResults.f0.size()));
		} catch (Exception e) {
			Assert.fail("Exception occurs in UncheckpointedCollectClient");
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public List<T> getResults() {
		return results;
	}
}
