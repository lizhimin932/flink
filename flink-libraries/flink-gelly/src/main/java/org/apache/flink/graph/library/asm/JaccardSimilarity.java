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

package org.apache.flink.graph.library.asm;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeTargetDegree;
import org.apache.flink.graph.library.asm.JaccardSimilarity.Result;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.MathUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * The Jaccard Index measures the similarity between vertex neighborhoods.
 * Scores range from 0.0 (no common neighbors) to 1.0 (all neighbors are common).
 * <br/>
 * This implementation produces similarity scores for each pair of vertices
 * in the graph with at least one common neighbor; equivalently, this is the
 * set of all non-zero Jaccard Similarity coefficients.
 * <br/>
 * The input graph must be a simple, undirected graph containing no duplicate
 * edges or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class JaccardSimilarity<K extends CopyableValue<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<Result<K>>> {

	public static final int DEFAULT_GROUP_SIZE = 64;

	// Optional configuration
	private int groupSize = DEFAULT_GROUP_SIZE;

	private int littleParallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * Override the default group size for the quadratic expansion of neighbor
	 * pairs. Small groups generate more data whereas large groups distribute
	 * computation less evenly among tasks.
	 *
	 * @param groupSize the group size for the quadratic expansion of neighbor pairs
	 * @return this
	 */
	public JaccardSimilarity<K, VV, EV> setGroupSize(int groupSize) {
		this.groupSize = groupSize;

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public JaccardSimilarity<K, VV, EV> setLittleParallelism(int littleParallelism) {
		this.littleParallelism = littleParallelism;

		return this;
	}

	/*
	 * Implementation notes:
	 *
	 * The requirement that "K extends CopyableValue<K>" can be removed when
	 *   Flink has a self-join which performs the skew distribution handled by
	 *   GenerateGroupSpans / GenerateGroups / GenerateGroupPairs.
	 */

	@Override
	public DataSet<Result<K>> run(Graph<K, VV, EV> input)
			throws Exception {
		// s, t, d(t)
		DataSet<Edge<K, Tuple2<EV, LongValue>>> neighborDegree = input
			.run(new EdgeTargetDegree<K, VV, EV>()
				.setParallelism(littleParallelism));

		// group span, s, t, d(t)
		DataSet<Tuple4<IntValue, K, K, IntValue>> groupSpans = neighborDegree
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GenerateGroupSpans<K, EV>(groupSize))
				.setParallelism(littleParallelism)
				.name("Generate group spans");

		// group, s, t, d(t)
		DataSet<Tuple4<IntValue, K, K, IntValue>> groups = groupSpans
			.rebalance()
				.setParallelism(littleParallelism)
				.name("Rebalance")
			.flatMap(new GenerateGroups<K>())
				.setParallelism(littleParallelism)
				.name("Generate groups");

		// t, u, d(t)+d(u)
		DataSet<Tuple3<K, K, IntValue>> twoPaths = groups
			.groupBy(0, 1)
			.sortGroup(2, Order.ASCENDING)
			.reduceGroup(new GenerateGroupPairs<K>(groupSize))
				.name("Generate group pairs");

		// t, u, intersection, union
		return twoPaths
			.groupBy(0, 1)
			.reduceGroup(new ComputeScores<K>())
				.name("Compute scores");
	}

	/**
	 * This is the first of three operations implementing a self-join to generate
	 * the full neighbor pairing for each vertex. The number of neighbor pairs
	 * is (n choose 2) which is quadratic in the vertex degree.
	 * <br/>
	 * The third operation, {@link GenerateGroupPairs}, processes groups of size
	 * {@link #groupSize} and emits {@code O(groupSize * deg(vertex))} pairs.
	 * <br/>
	 * This input to the third operation is still quadratic in the vertex degree.
	 * Two prior operations, {@link GenerateGroupSpans} and {@link GenerateGroups},
	 * each emit datasets linear in the vertex degree, with a forced rebalance
	 * in between. {@link GenerateGroupSpans} first annotates each edge with the
	 * number of groups and {@link GenerateGroups} emits each edge into each group.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFields("0->1; 1->2")
	private static class GenerateGroupSpans<T, ET>
	implements GroupReduceFunction<Edge<T, Tuple2<ET, LongValue>>, Tuple4<IntValue, T, T, IntValue>> {
		private final int groupSize;

		private IntValue groupSpansValue = new IntValue();

		private Tuple4<IntValue, T, T, IntValue> output = new Tuple4<>(groupSpansValue, null, null, new IntValue());

		public GenerateGroupSpans(int groupSize) {
			this.groupSize = groupSize;
		}

		@Override
		public void reduce(Iterable<Edge<T, Tuple2<ET, LongValue>>> values, Collector<Tuple4<IntValue, T, T, IntValue>> out)
				throws Exception {
			int groupCount = 0;
			int groupSpans = 1;

			groupSpansValue.setValue(groupSpans);

			for (Edge<T, Tuple2<ET, LongValue>> edge : values) {
				long degree = edge.f2.f1.getValue();
				if (degree > Integer.MAX_VALUE) {
					throw new RuntimeException("Degree overflows IntValue");
				}

				// group span, u, v, d(u)
				output.f1 = edge.f0;
				output.f2 = edge.f1;
				output.f3.setValue((int)degree);

				out.collect(output);

				if (++groupCount == groupSize) {
					groupCount = 0;
					groupSpansValue.setValue(++groupSpans);
				}
			}
		}
	}

	/**
	 * Emits the input tuple into each group within its group span.
	 *
	 * @param <T> ID type
	 *
	 * @see GenerateGroupSpans
	 */
	@FunctionAnnotation.ForwardedFields("1; 2; 3")
	private static class GenerateGroups<T>
	implements FlatMapFunction<Tuple4<IntValue, T, T, IntValue>, Tuple4<IntValue, T, T, IntValue>> {
		@Override
		public void flatMap(Tuple4<IntValue, T, T, IntValue> value, Collector<Tuple4<IntValue, T, T, IntValue>> out)
				throws Exception {
			int spans = value.f0.getValue();

			for (int idx = 0 ; idx < spans ; idx++ ) {
				value.f0.setValue(idx);
				out.collect(value);
			}
		}
	}

	/**
	 * Emits the two-path for all neighbor pairs in this group.
	 * <br/>
	 * The first {@link #groupSize} vertices are emitted pairwise. Following
	 * vertices are only paired with vertices from this initial group.
	 *
	 * @param <T> ID type
	 *
	 * @see GenerateGroupSpans
	 */
	private static class GenerateGroupPairs<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple4<IntValue, T, T, IntValue>, Tuple3<T, T, IntValue>> {
		private final int groupSize;

		private boolean initialized = false;

		private List<Tuple3<T, T, IntValue>> visited;

		public GenerateGroupPairs(int groupSize) {
			this.groupSize = groupSize;
			this.visited = new ArrayList<>(groupSize);
		}

		@Override
		public void reduce(Iterable<Tuple4<IntValue, T, T, IntValue>> values, Collector<Tuple3<T, T, IntValue>> out)
				throws Exception {
			int visitedCount = 0;

			for (Tuple4<IntValue, T, T, IntValue> edge : values) {
				for (int i = 0 ; i < visitedCount ; i++) {
					Tuple3<T, T, IntValue> prior = visited.get(i);

					prior.f1 = edge.f2;

					int oldValue = prior.f2.getValue();

					long degreeSum = oldValue + edge.f3.getValue();
					if (degreeSum > Integer.MAX_VALUE) {
						throw new RuntimeException("Degree sum overflows IntValue");
					}
					prior.f2.setValue((int)degreeSum);

					// v, w, d(v) + d(w)
					out.collect(prior);

					prior.f2.setValue(oldValue);
				}

				if (visitedCount < groupSize) {
					if (! initialized) {
						initialized = true;

						for (int i = 0 ; i < groupSize ; i++) {
							Tuple3<T, T, IntValue> tuple = new Tuple3<>();

							tuple.f0 = edge.f2.copy();
							tuple.f2 = edge.f3.copy();

							visited.add(tuple);
						}
					} else {
						Tuple3<T, T, IntValue> copy = visited.get(visitedCount);

						edge.f2.copyTo(copy.f0);
						edge.f3.copyTo(copy.f2);
					}

					visitedCount += 1;
				}
			}
		}
	}

	/**
	 * Compute the counts of common and distinct neighbors. A two-path connecting
	 * the vertices is emitted for each common neighbor. The number of distinct
	 * neighbors is equal to the sum of degrees of the vertices minus the count
	 * of common numbers, which are double-counted in the degree sum.
	 *
	 * @param <T> ID type
	 */
	@FunctionAnnotation.ForwardedFields("0; 1")
	private static class ComputeScores<T>
	implements GroupReduceFunction<Tuple3<T, T, IntValue>, Result<T>> {
		private Result<T> output = new Result<>();

		@Override
		public void reduce(Iterable<Tuple3<T, T, IntValue>> values, Collector<Result<T>> out)
				throws Exception {
			int count = 0;
			Tuple3<T, T, IntValue> edge = null;

			for (Tuple3<T, T, IntValue> next : values) {
				edge = next;
				count += 1;
			}

			output.f0 = edge.f0;
			output.f1 = edge.f1;
			output.f2.f0.setValue(count);
			output.f2.f1.setValue(edge.f2.getValue() - count);
			out.collect(output);
		}
	}

	/**
	 * Wraps the vertex type to encapsulate results from the Jaccard Similarity algorithm.
	 *
	 * @param <T> ID type
	 */
	public static class Result<T>
	extends Edge<T, Tuple2<IntValue, IntValue>> {
		public static final int HASH_SEED = 0x731f73e7;

		public Result() {
			f2 = new Tuple2<>(new IntValue(), new IntValue());
		}

		/**
		 * Get the common neighbor count.
		 *
		 * @return common neighbor count
		 */
		public IntValue getCommonNeighborCount() {
			return f2.f0;
		}

		/**
		 * Get the distinct neighbor count.
		 *
		 * @return distinct neighbor count
		 */
		public IntValue getDistinctNeighborCount() {
			return f2.f1;
		}

		/**
		 * Get the Jaccard Similarity score, equal to the number of common
		 * neighbors shared by the source and target vertices divided by the
		 * number of distinct neighbors.
		 *
		 * @return Jaccard Similarity score
		 */
		public double getJaccardSimilarityScore() {
			return getCommonNeighborCount().getValue() / (double) getDistinctNeighborCount().getValue();
		}

		@Override
		public int hashCode() {
			return MathUtils.murmurHash(HASH_SEED, f0.hashCode(), f1.hashCode(), f2.f0.hashCode(), f2.f1.hashCode());
		}
	}
}
