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

package org.apache.flink.cep.nfa.aftermatch;

import org.apache.flink.cep.nfa.sharedbuffer.EventId;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Discards every partial match that contains event of the match preceding the last of *PatternName*.
 */
public class SkipToLastStrategy extends AfterMatchSkipStrategy {
	private static final long serialVersionUID = 7585116990619594531L;
	private final String patternName;
	private boolean shouldThrowException = false;

	SkipToLastStrategy(String patternName) {
		this.patternName = checkNotNull(patternName);
	}

	@Override
	public boolean isSkipStrategy() {
		return true;
	}

	@Override
	protected boolean shouldPrune(EventId startEventID, EventId pruningId) {
		return startEventID != null && startEventID.compareTo(pruningId) < 0;
	}

	@Override
	protected EventId getPruningId(Collection<Map<String, List<EventId>>> match) {
		EventId pruningId = null;
		for (Map<String, List<EventId>> resultMap : match) {
			List<EventId> pruningPattern = resultMap.get(patternName);
			if (pruningPattern == null || pruningPattern.isEmpty()) {
				if (shouldThrowException) {
					throw new FlinkRuntimeException(String.format(
						"Could not skip to %s. No such element in the found match %s",
						patternName,
						resultMap));
				}
			} else {
				pruningId = max(pruningId, pruningPattern.get(pruningPattern.size() - 1));
			}
		}

		return pruningId;
	}

	@Override
	public Optional<String> getPatternName() {
		return Optional.of(patternName);
	}

	/**
	 * Enables throwing exception if no events mapped to the *PatternName*. If not enabled and no events were mapped,
	 * {@link NoSkipStrategy} will be used
	 */
	public SkipToLastStrategy throwExceptionOnMiss() {
		this.shouldThrowException = true;
		return this;
	}

	@Override
	public String toString() {
		return "SkipToLastStrategy{" +
			"patternName='" + patternName + '\'' +
			'}';
	}
}
