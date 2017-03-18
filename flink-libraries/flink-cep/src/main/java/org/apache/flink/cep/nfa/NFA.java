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

package org.apache.flink.cep.nfa;

import com.google.common.collect.LinkedHashMultimap;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.cep.NonDuplicatingTypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Non-deterministic finite automaton implementation.
 * <p>
 * The {@link org.apache.flink.cep.operator.AbstractKeyedCEPPatternOperator CEP operator}
 * keeps one NFA per key, for keyed input streams, and a single global NFA for non-keyed ones.
 * When an event gets processed, it updates the NFA's internal state machine.
 * <p>
 * An event that belongs to a partially matched sequence is kept in an internal
 * {@link SharedBuffer buffer}, which is a memory-optimized data-structure exactly for
 * this purpose. Events in the buffer are removed when all the matched sequences that
 * contain them are:
 * <ol>
 *     <li>emitted (success)</li>
 *     <li>discarded (patterns containing NOT)</li>
 *     <li>timed-out (windowed patterns)</li>
 * </ol>
 *
 * The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 *     https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <T> Type of the processed events
 */
public class NFA<T> implements Serializable {

	private static final long serialVersionUID = 2957674889294717265L;

	private static final Pattern namePattern = Pattern.compile("^(.*\\[)(\\])$");

	private final NonDuplicatingTypeSerializer<T> nonDuplicatingTypeSerializer;

	/**
	 * 	Buffer used to store the matched events.
	 */
	private final SharedBuffer<String, T> sharedBuffer;

	/**
	 * A set of all the valid NFA states, as returned by the
	 * {@link org.apache.flink.cep.nfa.compiler.NFACompiler NFACompiler}.
	 * These are directly derived from the user-specified pattern.
	 */
	private final Set<State<T>> states;

	/**
	 * The length of a windowed pattern, as specified using the
	 * {@link org.apache.flink.cep.pattern.Pattern#within(Time) Pattern.within(Time)}
	 * method.
	 */
	private final long windowTime;

	/**
	 * A flag indicating if we want timed-out patterns (in case of windowed patterns)
	 * to be emitted ({@code true}), or silently discarded ({@code false}).
	 */
	private final boolean handleTimeout;

	/**
	 * Current set of {@link ComputationState computation states} within the state machine.
	 * These are the "active" intermediate states that are waiting for new matching
	 * events to transition to new valid states.
	 */
	private transient Queue<ComputationState<T>> computationStates;

	public NFA(
			final TypeSerializer<T> eventSerializer,
			final long windowTime,
			final boolean handleTimeout) {

		this.nonDuplicatingTypeSerializer = new NonDuplicatingTypeSerializer<>(eventSerializer);
		this.windowTime = windowTime;
		this.handleTimeout = handleTimeout;
		sharedBuffer = new SharedBuffer<>(nonDuplicatingTypeSerializer);
		computationStates = new LinkedList<>();

		states = new HashSet<>();
	}

	public Set<State<T>> getStates() {
		return states;
	}

	public void addStates(final Collection<State<T>> newStates) {
		for (State<T> state: newStates) {
			addState(state);
		}
	}

	public void addState(final State<T> state) {
		states.add(state);

		if (state.isStart()) {
			computationStates.add(ComputationState.createStartState(state));
		}
	}

	/**
	 * Check if the NFA has finished processing all incoming data so far. That is
	 * when the buffer keeping the matches is empty.
	 *
	 * @return {@code true} if there are no elements in the {@link SharedBuffer},
	 * {@code false} otherwise.
	 */
	public boolean isEmpty() {
		return sharedBuffer.isEmpty();
	}

	/**
	 * Processes the next input event. If some of the computations reach a final state then the
	 * resulting event sequences are returned. If computations time out and timeout handling is
	 * activated, then the timed out event patterns are returned.
	 *
	 * @param event The current event to be processed or null if only pruning shall be done
	 * @param timestamp The timestamp of the current event
	 * @return Tuple of the collection of matched patterns (e.g. the result of computations which have
	 * reached a final state) and the collection of timed out patterns (if timeout handling is
	 * activated)
	 */
	public Tuple2<Collection<Map<String, T>>, Collection<Tuple2<Map<String, T>, Long>>> process(final T event, final long timestamp) {
		final int numberComputationStates = computationStates.size();
		final Collection<Map<String, T>> result = new ArrayList<>();
		final Collection<Tuple2<Map<String, T>, Long>> timeoutResult = new ArrayList<>();

		// iterate over all current computations
		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = computationStates.poll();

			final Collection<ComputationState<T>> newComputationStates;

			if (!computationState.isStartState() &&
				windowTime > 0L &&
				timestamp - computationState.getStartTimestamp() >= windowTime) {

				if (handleTimeout) {
					// extract the timed out event patterns
					Collection<Map<String, T>> timeoutPatterns = extractPatternMatches(computationState);

					for (Map<String, T> timeoutPattern : timeoutPatterns) {
						timeoutResult.add(Tuple2.of(timeoutPattern, timestamp));
					}
				}

				// remove computation state which has exceeded the window length
				sharedBuffer.release(computationState.getState().getName(), computationState.getEvent(), computationState.getTimestamp());
				sharedBuffer.remove(computationState.getState().getName(), computationState.getEvent(), computationState.getTimestamp());

				newComputationStates = Collections.emptyList();
			} else if (event != null) {
				newComputationStates = computeNextStates(computationState, event, timestamp);
			} else {
				newComputationStates = Collections.singleton(computationState);
			}

			for (ComputationState<T> newComputationState: newComputationStates) {
				if (newComputationState.isFinalState()) {
					// we've reached a final state and can thus retrieve the matching event sequence
					Collection<Map<String, T>> matches = extractPatternMatches(newComputationState);
					result.addAll(matches);

					// remove found patterns because they are no longer needed
					sharedBuffer.release(newComputationState.getPreviousState().getName(), newComputationState.getEvent(), newComputationState.getTimestamp());
					sharedBuffer.remove(newComputationState.getPreviousState().getName(), newComputationState.getEvent(), newComputationState.getTimestamp());
				} else {
					// add new computation state; it will be processed once the next event arrives
					computationStates.add(newComputationState);
				}
			}
		}

		// prune shared buffer based on window length
		if(windowTime > 0L) {
			long pruningTimestamp = timestamp - windowTime;

			if (pruningTimestamp < timestamp) {
				// the check is to guard against underflows

				// remove all elements which are expired
				// with respect to the window length
				sharedBuffer.prune(pruningTimestamp);
			}
		}

		return Tuple2.of(result, timeoutResult);
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NFA) {
			@SuppressWarnings("unchecked")
			NFA<T> other = (NFA<T>) obj;

			return nonDuplicatingTypeSerializer.equals(other.nonDuplicatingTypeSerializer) &&
				sharedBuffer.equals(other.sharedBuffer) &&
				states.equals(other.states) &&
				windowTime == other.windowTime;
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(nonDuplicatingTypeSerializer, sharedBuffer, states, windowTime);
	}

	private static <T> boolean isEquivalentState(final State<T> s1, final State<T> s2) {
		return s1.getName().equals(s2.getName());
	}

	/**
	 * Class for storing resolved transitions. It counts at insert time the number of
	 * branching transitions both for IGNORE and TAKE actions.
 	 */
	private static class OutgoingEdges<T> {
		private List<StateTransition<T>> edges = new ArrayList<>();

		private final State<T> currentState;

		private int totalTakeBranches = 0;
		private int totalIgnoreBranches = 0;

		OutgoingEdges(final State<T> currentState) {
			this.currentState = currentState;
		}

		void add(StateTransition<T> edge) {

			if (!isSelfIgnore(edge)) {
				if (edge.getAction() == StateTransitionAction.IGNORE) {
					totalIgnoreBranches++;
				} else if (edge.getAction() == StateTransitionAction.TAKE) {
					totalTakeBranches++;
				}
			}

			edges.add(edge);
		}

		int getTotalIgnoreBranches() {
			return totalIgnoreBranches;
		}
		int getTotalTakeBranches() {
			return totalTakeBranches;
		}

		List<StateTransition<T>> getEdges() {
			return edges;
		}

		private boolean isSelfIgnore(final StateTransition<T> edge) {
			return isEquivalentState(edge.getTargetState(), currentState) &&
				edge.getAction() == StateTransitionAction.IGNORE;
		}
	}


	/**
	 * Computes the next computation states based on the given computation state, the current event,
	 * its timestamp and the internal state machine. The algorithm is:
	 *
	 * 1. Decide on valid transitions and number of branching paths. See {@link OutgoingEdges}
	 * 2. Perform transitions:
	 *      a) IGNORE (links in {@link SharedBuffer} will still point to the previous event)
	 *          - do not perform for Start State - special case
	 *          - if stays in the same state increase the current stage for future use with number of
	 *            outgoing edges
	 *          - if after PROCEED increase current stage and add new stage (as we change the state)
	 *          - lock the entry in {@link SharedBuffer} as it is needed in the created branch
	 *      b) TAKE (links in {@link SharedBuffer} will point to the current event)
	 *          - add entry to the shared buffer with version of the current computation state
	 *          - add stage and then increase with number of takes for the future computation states
	 *          - peek to the next state if it has PROCEED path to a Final State, if true create
	 *            Final ComputationState to emit results
	 * 3. Handle the Start State, as it always have to remain
	 * 4. Release the corresponding entries in {@link SharedBuffer}.
	 *
	 *
	 * @param computationState Current computation state
	 * @param event Current event which is processed
	 * @param timestamp Timestamp of the current event
	 * @return Collection of computation states which result from the current one
	 */
	private Collection<ComputationState<T>> computeNextStates(
			final ComputationState<T> computationState,
			final T event,
			final long timestamp) {

		final OutgoingEdges<T> outgoingEdges = createDecisionGraph(computationState, event);

		// Create the computing version based on the previously computed edges
		// We need to defer the creation of computation states until we know how many edges start
		// at this computation state so that we can assign proper version
		final List<StateTransition<T>> edges = outgoingEdges.getEdges();
		int takeBranchesToVisit = Math.max(0, outgoingEdges.getTotalTakeBranches() - 1);
		int ignoreBranchesToVisit = outgoingEdges.getTotalIgnoreBranches();

		final List<ComputationState<T>> resultingComputationStates = new ArrayList<>();
		for (StateTransition<T> edge : edges) {
			switch (edge.getAction()) {
				case IGNORE: {
					if (!computationState.isStartState()) {
						final DeweyNumber version;
						if (isEquivalentState(edge.getTargetState(), computationState.getState())) {
							//Stay in the same state (it can be either looping one or singleton)
							final int toIncrease = calculateIncreasingSelfState(
								outgoingEdges.getTotalIgnoreBranches(),
								outgoingEdges.getTotalTakeBranches());
							version = computationState.getVersion().increase(toIncrease);
						} else {
							//IGNORE after PROCEED
							version = computationState.getVersion().increase(ignoreBranchesToVisit).addStage();
							ignoreBranchesToVisit--;
						}

						resultingComputationStates.add(
							ComputationState.createState(
								edge.getTargetState(),
								computationState.getPreviousState(),
								computationState.getEvent(),
								computationState.getTimestamp(),
								version,
								computationState.getStartTimestamp()
							)
						);
						sharedBuffer.lock(
							computationState.getPreviousState().getName(),
							computationState.getEvent(),
							computationState.getTimestamp());
					}
				}
				break;
				case TAKE:
					final State<T> newState = edge.getTargetState();
					final State<T> consumingState = edge.getSourceState();
					final State<T> previousEventState = computationState.getPreviousState();

					final T previousEvent = computationState.getEvent();
					final DeweyNumber currentVersion = computationState.getVersion();

					final DeweyNumber newComputationStateVersion = new DeweyNumber(currentVersion).addStage().increase(takeBranchesToVisit);
					takeBranchesToVisit--;

					final long startTimestamp;
					if (computationState.isStartState()) {
						startTimestamp = timestamp;
						sharedBuffer.put(
							consumingState.getName(),
							event,
							timestamp,
							currentVersion);
					} else {
						startTimestamp = computationState.getStartTimestamp();
						sharedBuffer.put(
							consumingState.getName(),
							event,
							timestamp,
							previousEventState.getName(),
							previousEvent,
							computationState.getTimestamp(),
							currentVersion);
					}

					// a new computation state is referring to the shared entry
					sharedBuffer.lock(consumingState.getName(), event, timestamp);

					resultingComputationStates.add(ComputationState.createState(
						newState,
						consumingState,
						event,
						timestamp,
						newComputationStateVersion,
						startTimestamp
					));

					//check if newly created state is optional (have a PROCEED path to Final state)
					final State<T> finalState = findFinalStateAfterProceed(newState);
					if (finalState != null) {
						sharedBuffer.lock(consumingState.getName(), event, timestamp);
						resultingComputationStates.add(ComputationState.createState(
							finalState,
							consumingState,
							event,
							timestamp,
							newComputationStateVersion,
							startTimestamp));
					}
					break;
			}
		}

		if (computationState.isStartState()) {
			final int totalBranches = calculateIncreasingSelfState(outgoingEdges.getTotalIgnoreBranches(), outgoingEdges.getTotalTakeBranches());
			final ComputationState<T> startState = createStartState(computationState, totalBranches);
			resultingComputationStates.add(startState);
		}

		if (computationState.getEvent() != null) {
			// release the shared entry referenced by the current computation state.
			sharedBuffer.release(
				computationState.getPreviousState().getName(),
				computationState.getEvent(),
				computationState.getTimestamp());
			// try to remove unnecessary shared buffer entries
			sharedBuffer.remove(
				computationState.getPreviousState().getName(),
				computationState.getEvent(),
				computationState.getTimestamp());
		}

		return resultingComputationStates;
	}

	private State<T> findFinalStateAfterProceed(State<T> state) {
		final Stack<State<T>> statesToCheck = new Stack<>();
		statesToCheck.push(state);

		while (!statesToCheck.isEmpty()) {
			final State<T> currentState = statesToCheck.pop();
			for (StateTransition<T> transition : currentState.getStateTransitions()) {
				if (transition.getAction() == StateTransitionAction.PROCEED) {
					if (transition.getTargetState().isFinal()) {
						return transition.getTargetState();
					} else {
						statesToCheck.push(transition.getTargetState());
					}
				}
			}
		}


		return null;
	}

	private int calculateIncreasingSelfState(int ignoreBranches, int takeBranches) {
		return takeBranches == 0 && ignoreBranches == 0 ? 0 : ignoreBranches + 1;
	}

	private ComputationState<T> createStartState(final ComputationState<T> computationState, final int totalBranches) {
		final DeweyNumber startVersion = computationState.getVersion().increase(totalBranches);
		return ComputationState.createStartState(computationState.getState(), startVersion);
	}

	private OutgoingEdges<T> createDecisionGraph(ComputationState<T> computationState, T event) {
		final Stack<State<T>> states = new Stack<>();
		states.push(computationState.getState());
		final OutgoingEdges<T> outgoingEdges = new OutgoingEdges<>(computationState.getState());
		//First create all outgoing edges, so to be able to reason about the Dewey version
		while (!states.isEmpty()) {
			State<T> currentState = states.pop();
			Collection<StateTransition<T>> stateTransitions = currentState.getStateTransitions();

			// check all state transitions for each state
			for (StateTransition<T> stateTransition : stateTransitions) {
				try {
					if (checkFilterCondition(stateTransition.getCondition(), event)) {
						// filter condition is true
						switch (stateTransition.getAction()) {
							case PROCEED:
								// simply advance the computation state, but apply the current event to it
								// PROCEED is equivalent to an epsilon transition
								states.push(stateTransition.getTargetState());
								break;
							case IGNORE:
							case TAKE:
								outgoingEdges.add(stateTransition);
								break;
						}
					}
				} catch (Exception e) {
					throw new RuntimeException("Failure happened in filter function.", e);
				}
			}
		}
		return outgoingEdges;
	}


	private boolean checkFilterCondition(FilterFunction<T> condition, T event) throws Exception {
		return condition == null || condition.filter(event);
	}

	/**
	 * Extracts all the sequences of events from the start to the given computation state. An event
	 * sequence is returned as a map which contains the events and the names of the states to which
	 * the events were mapped.
	 *
	 * @param computationState The end computation state of the extracted event sequences
	 * @return Collection of event sequences which end in the given computation state
	 */
	private Collection<Map<String, T>> extractPatternMatches(final ComputationState<T> computationState) {
		Collection<LinkedHashMultimap<String, T>> paths = sharedBuffer.extractPatterns(
			computationState.getPreviousState().getName(),
			computationState.getEvent(),
			computationState.getTimestamp(),
			computationState.getVersion());

		List<Map<String, T>> result = new ArrayList<>();

		TypeSerializer<T> serializer = nonDuplicatingTypeSerializer.getTypeSerializer();

		// generate the correct names from the collection of LinkedHashMultimaps
		for (LinkedHashMultimap<String, T> path: paths) {
			Map<String, T> resultPath = new HashMap<>();
			for (String key: path.keySet()) {
				int counter = 0;
				Set<T> events = path.get(key);

				// we iterate over the elements in insertion order
				for (T event: events) {
					resultPath.put(
						events.size() > 1 ? generateStateName(key, counter): key,
						// copy the element so that the user can change it
						serializer.isImmutableType() ? event : serializer.copy(event)
					);
					counter++;
				}
			}

			result.add(resultPath);
		}

		return result;
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		oos.defaultWriteObject();

		oos.writeInt(computationStates.size());

		for(ComputationState<T> computationState: computationStates) {
			writeComputationState(computationState, oos);
		}

		nonDuplicatingTypeSerializer.clearReferences();
	}

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		ois.defaultReadObject();

		int numberComputationStates = ois.readInt();

		computationStates = new LinkedList<>();

		for (int i = 0; i < numberComputationStates; i++) {
			ComputationState<T> computationState = readComputationState(ois);

			computationStates.offer(computationState);
		}

		nonDuplicatingTypeSerializer.clearReferences();
	}

	private void writeComputationState(final ComputationState<T> computationState, final ObjectOutputStream oos) throws IOException {
		oos.writeObject(computationState.getState());
		oos.writeObject(computationState.getPreviousState());
		oos.writeLong(computationState.getTimestamp());
		oos.writeObject(computationState.getVersion());
		oos.writeLong(computationState.getStartTimestamp());

		if (computationState.getEvent() == null) {
			// write that we don't have an event associated
			oos.writeBoolean(false);
		} else {
			// write that we have an event associated
			oos.writeBoolean(true);
			DataOutputViewStreamWrapper output = new DataOutputViewStreamWrapper(oos);
			nonDuplicatingTypeSerializer.serialize(computationState.getEvent(), output);
		}
	}

	@SuppressWarnings("unchecked")
	private ComputationState<T> readComputationState(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		final State<T> state = (State<T>)ois.readObject();
		final State<T> previousState = (State<T>)ois.readObject();
		final long timestamp = ois.readLong();
		final DeweyNumber version = (DeweyNumber)ois.readObject();
		final long startTimestamp = ois.readLong();

		final boolean hasEvent = ois.readBoolean();
		final T event;

		if (hasEvent) {
			DataInputViewStreamWrapper input = new DataInputViewStreamWrapper(ois);
			event = nonDuplicatingTypeSerializer.deserialize(input);
		} else {
			event = null;
		}

		return ComputationState.createState(state, previousState, event, timestamp, version, startTimestamp);
	}

	/**
	 * Generates a state name from a given name template and an index.
	 * <p>
	 * If the template ends with "[]" the index is inserted in between the square brackets.
	 * Otherwise, an underscore and the index is appended to the name.
	 *
	 * @param name Name template
	 * @param index Index of the state
	 * @return Generated state name from the given state name template
	 */
	static String generateStateName(final String name, final int index) {
		Matcher matcher = namePattern.matcher(name);

		if (matcher.matches()) {
			return matcher.group(1) + index + matcher.group(2);
		} else {
			return name + "_" + index;
		}
	}

	/**
	 * {@link TypeSerializer} for {@link NFA} that uses Java Serialization.
	 */
	public static class Serializer<T> extends TypeSerializer<NFA<T>> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<NFA<T>> duplicate() {
			return this;
		}

		@Override
		public NFA<T> createInstance() {
			return null;
		}

		@Override
		public NFA<T> copy(NFA<T> from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				oos.writeObject(from);

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				@SuppressWarnings("unchecked")
				NFA<T> copy = (NFA<T>) ois.readObject();
				return copy;
			} catch (IOException|ClassNotFoundException e) {
				throw new RuntimeException("Could not copy NFA.", e);
			}
		}

		@Override
		public NFA<T> copy(NFA<T> from, NFA<T> reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(NFA<T> record, DataOutputView target) throws IOException {
			ObjectOutputStream oos = new ObjectOutputStream(new DataOutputViewStream(target));
			oos.writeObject(record);
			oos.flush();
		}

		@Override
		public NFA<T> deserialize(DataInputView source) throws IOException {
			ObjectInputStream ois = new ObjectInputStream(new DataInputViewStream(source));

			try {
				return (NFA<T>) ois.readObject();
			} catch (ClassNotFoundException e) {
				throw new RuntimeException("Could not deserialize NFA.", e);
			}
		}

		@Override
		public NFA<T> deserialize(NFA<T> reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			int size = source.readInt();
			target.writeInt(size);
			target.write(source, size);
		}

		@Override
		public boolean equals(Object obj) {
			return obj instanceof Serializer && ((Serializer) obj).canEqual(this);
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof Serializer;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}
}
