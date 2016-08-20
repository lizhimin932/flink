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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.Collection;
import java.util.Map;

/**
 * CEP pattern operator which only returns fully matched event patterns and partially matched event
 * patterns which have timed out wrapped in {@link Either}. The matched events are stored in a
 * {@link Map} and are indexed by the event names associated in the pattern specification.
 *
 * The fully matched event patterns are returned as a {@link Either.Right} instance and the
 * partially matched event patterns are returned as a {@link Either.Left} instance.
 *
 * @param <IN> Type of the input events
 */
public class TimeoutCEPPatternOperator<IN> extends AbstractCEPPatternOperator<IN, Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> {
	private static final long serialVersionUID = -3911002597290988201L;

	public TimeoutCEPPatternOperator(
		TypeSerializer<IN> inputSerializer,
		ProcessingType processingType,
		NFACompiler.NFAFactory<IN> nfaFactory) {

		super(inputSerializer, processingType, nfaFactory);
	}

	@Override
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns = nfa.process(
			event,
			timestamp);

		Collection<Map<String, IN>> matchedPatterns = patterns.f0;
		Collection<Tuple2<Map<String, IN>, Long>> partialPatterns = patterns.f1;

		emitMatchedSequences(matchedPatterns, timestamp);
		emitTimedOutSequences(partialPatterns, timestamp);
	}

	@Override
	protected void advanceTime(NFA<IN> nfa, long timestamp) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns = nfa.process(null, timestamp);

		emitMatchedSequences(patterns.f0, timestamp);
		emitTimedOutSequences(patterns.f1, timestamp);
	}

	private void emitTimedOutSequences(Iterable<Tuple2<Map<String, IN>, Long>> timedOutSequences, long timestamp) {
		StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> streamRecord = new StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>>(
			null,
			timestamp);

		for (Tuple2<Map<String, IN>, Long> partialPattern: timedOutSequences) {
			streamRecord.replace(Either.Left(partialPattern));
			output.collect(streamRecord);
		}
	}

	protected void emitMatchedSequences(Iterable<Map<String, IN>> matchedSequences, long timestamp) {
		StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> streamRecord = new StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>>(
			null,
			timestamp);

		for (Map<String, IN> matchedPattern : matchedSequences) {
			streamRecord.replace(Either.Right(matchedPattern));
			output.collect(streamRecord);
		}
	}
}
