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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.GraphAlgorithm;
import org.apache.flink.graph.asm.degree.annotate.undirected.EdgeDegreePair;
import org.apache.flink.types.CopyableValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generates a listing of distinct triangles from the input graph.
 * <br/>
 * A triangle is a 3-clique with vertices A, B, and C connected by edges
 * (A, B), (A, C), and (B, C).
 * <br/>
 * The input graph must be a simple graph of undirected edges containing
 * no duplicates or self-loops.
 *
 * @param <K> graph ID type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class TriangleListing<K extends Comparable<K> & CopyableValue<K>, VV, EV>
implements GraphAlgorithm<K, VV, EV, DataSet<Tuple3<K, K, K>>> {

	// Optional configuration
	private boolean sortTriangleVertices = false;

	private int littleParallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * Normalize the triangle listing such that for each triangle <K0, K1, K2>
	 * the vertex IDs are sorted K0 < K1 < K2.
	 *
	 * @param sortTriangleVertices whether to output the triangle vertices in sorted order
	 * @return this
	 */
	public TriangleListing<K, VV, EV> setSortTriangleVertices(boolean sortTriangleVertices) {
		this.sortTriangleVertices = sortTriangleVertices;

		return this;
	}

	/**
	 * Override the parallelism of operators processing small amounts of data.
	 *
	 * @param littleParallelism operator parallelism
	 * @return this
	 */
	public TriangleListing<K, VV, EV> setLittleParallelism(int littleParallelism) {
		this.littleParallelism = littleParallelism;

		return this;
	}

	@Override
	public DataSet<Tuple3<K, K, K>> run(Graph<K, VV, EV> input)
			throws Exception {
		// u, v where u < v
		DataSet<Tuple2<K, K>> filteredByID = input
			.getEdges()
			.flatMap(new FilterByID<K, EV>())
				.setParallelism(littleParallelism)
				.name("Filter by ID");;

		// u, v, (deg(u), deg(v))
		DataSet<Edge<K, Tuple3<EV, LongValue, LongValue>>> pairDegree = input
			.run(new EdgeDegreePair<K, VV, EV>()
				.setParallelism(littleParallelism));

		// u, v where deg(u) < deg(v) or (deg(u) == deg(v) and u < v)
		DataSet<Tuple2<K, K>> filteredByDegree = pairDegree
			.flatMap(new FilterByDegree<K, EV>())
				.setParallelism(littleParallelism)
				.name("Filter by degree");

		// u, v, w where (u, v) and (u, w) are edges in graph
		DataSet<Tuple3<K, K, K>> twoPaths = filteredByDegree
			.groupBy(0)
			.sortGroup(1, Order.ASCENDING)
			.reduceGroup(new GenerateTriplets<K>())
				.setParallelism(littleParallelism)
				.name("Generate triplets");

		// u, v, w where (u, v), (u, w), and (v, w) are edges in graph
		DataSet<Tuple3<K, K, K>> triangles = twoPaths
			.join(filteredByID, JoinOperatorBase.JoinHint.REPARTITION_HASH_SECOND)
			.where(1, 2)
			.equalTo(0, 1)
			.<Tuple3<K, K, K>>projectFirst(0, 1, 2)
				.name("Triangle listing");

		if (sortTriangleVertices) {
			triangles = triangles
				.map(new SortTriangleVertices<K>())
					.name("Sort triangle vertices");
		}

		return triangles;
	}

	/**
	 * Converts edges to 2-tuples while filtering such that only edges with
	 * vertex IDs in sorted order are emitted.
	 * <br/>
	 * Since the input graph is a simple graph this filter removes exactly half
	 * of the original edges.
	 *
	 * @param <T> ID type
	 * @param <ET> edge value type
	 */
	@ForwardedFields("0; 1")
	private static final class FilterByID<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, ET>, Tuple2<T, T>> {
		private Tuple2<T, T> edge = new Tuple2<>();

		@Override
		public void flatMap(Edge<T, ET> value, Collector<Tuple2<T, T>> out)
				throws Exception {
			if (value.f0.compareTo(value.f1) < 0) {
				edge.f0 = value.f0;
				edge.f1 = value.f1;
				out.collect(edge);
			}
		}
	}

	/**
	 * Converts edges to 2-tuples and filters such that only edges with
	 * vertices in degree order are emitted. If the vertex degrees are equal
	 * then the edge is emitted if the vertex IDs are in sorted order.
	 * <br/>
	 * Since the input graph is a simple graph this filter removes exactly half
	 * of the original edges.
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("0; 1")
	private static final class FilterByDegree<T extends Comparable<T>, ET>
	implements FlatMapFunction<Edge<T, Tuple3<ET, LongValue, LongValue>>, Tuple2<T, T>> {
		private Tuple2<T, T> edge = new Tuple2<>();

		@Override
		public void flatMap(Edge<T, Tuple3<ET, LongValue, LongValue>> value, Collector<Tuple2<T, T>> out)
				throws Exception {
			Tuple3<ET, LongValue, LongValue> degrees = value.f2;
			long sourceDegree = degrees.f1.getValue();
			long targetDegree = degrees.f2.getValue();

			if (sourceDegree < targetDegree ||
					(sourceDegree == targetDegree && value.f0.compareTo(value.f1) < 0)) {
				edge.f0 = value.f0;
				edge.f1 = value.f1;
				out.collect(edge);
			}

		}
	}

	/**
	 * Generates the set of triplets by
	 *
	 * @param <T> ID type
	 */
	@ForwardedFields("1->0")
	private static final class GenerateTriplets<T extends CopyableValue<T>>
	implements GroupReduceFunction<Tuple2<T, T>, Tuple3<T, T, T>> {

		private static final long serialVersionUID = 1L;

		private Tuple3<T, T, T> output = new Tuple3<>();

		private List<T> visited = new ArrayList<>();

		@Override
		public void reduce(Iterable<Tuple2<T, T>> values, Collector<Tuple3<T, T, T>> out)
				throws Exception {
			int visitedCount = 0;

			Iterator<Tuple2<T, T>> iter = values.iterator();

			while (true) {
				Tuple2<T, T> edge = iter.next();

				output.f0 = edge.f0;
				T target = edge.f1;
				output.f2 = target;

				for (int i = 0; i < visitedCount; i++) {
					output.f1 = visited.get(i);

					// u, v, w
					out.collect(output);
				}

				if (! iter.hasNext()) {
					break;
				}

				if (visitedCount == visited.size()) {
					visited.add(target.copy());
				} else {
					target.copyTo(visited.get(visitedCount));
				}

				visitedCount += 1;
			}
		}
	}

	/**
	 * Reorders the vertices of each emitted triangle <K0, K1, K2>
	 * into sorted order such that K0 < K1 < K2.
	 *
	 * @param <T> ID type
	 */
	private static final class SortTriangleVertices<T extends Comparable<T>>
	implements MapFunction<Tuple3<T, T, T>, Tuple3<T, T, T>> {
		@Override
		public Tuple3<T, T, T> map(Tuple3<T, T, T> value)
				throws Exception {
			T temp_val;

			if (value.f0.compareTo(value.f1) <= 0) {
				if (value.f1.compareTo(value.f2) <= 0) {
					// a, b, c
				} else {
					if (value.f0.compareTo(value.f2) < 0) {
						// a, c, b
						temp_val = value.f1;
						value.f1 = value.f2;
						value.f2 = temp_val;
					} else {
						// b, c, a
						temp_val = value.f0;
						value.f0 = value.f2;
						value.f2 = value.f1;
						value.f1 = temp_val;
					}
				}
			} else {
				if (value.f0.compareTo(value.f2) > 0) {
					if (value.f1.compareTo(value.f2) < 0) {
						// c, a, b
						temp_val = value.f0;
						value.f0 = value.f1;
						value.f1 = value.f2;
						value.f2 = temp_val;
					} else {
						// c, b, a
						temp_val = value.f0;
						value.f0 = value.f2;
						value.f2 = temp_val;
					}
				} else {
					// b, a, c
					temp_val = value.f0;
					value.f0 = value.f1;
					value.f1 = temp_val;
				}
			}

			return value;
		}
	}
}
