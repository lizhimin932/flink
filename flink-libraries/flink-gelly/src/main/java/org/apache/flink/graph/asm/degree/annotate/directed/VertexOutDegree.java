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

package org.apache.flink.graph.asm.degree.annotate.directed;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.CachingGraphAlgorithm;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.DegreeCount;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.JoinVertexWithVertexDegree;
import org.apache.flink.graph.asm.degree.annotate.DegreeAnnotationFunctions.MapEdgeToSourceLabel;
import org.apache.flink.types.LongValue;

/**
 * Annotates vertices of a directed graph with the out-degree count.
 *
 * @param <K> graph label type
 * @param <VV> vertex value type
 * @param <EV> edge value type
 */
public class VertexOutDegree<K, VV, EV>
extends CachingGraphAlgorithm<K, VV, EV, DataSet<Vertex<K,LongValue>>> {

	// Optional configuration
	private boolean includeZeroDegreeVertices = true;

	private int parallelism = ExecutionConfig.PARALLELISM_UNKNOWN;

	/**
	 * When set to true an additional join is performed against the vertex
	 * set in order to output vertices with zero out-degree.
	 *
	 * @param includeZeroDegreeVertices whether to output vertices with an
	 *                                  out-degree of zero
	 * @return this
	 */
	public VertexOutDegree<K,VV,EV> setIncludeZeroDegreeVertices(boolean includeZeroDegreeVertices) {
		this.includeZeroDegreeVertices = includeZeroDegreeVertices;

		return this;
	}

	/**
	 * Override the operator parallelism.
	 *
	 * @param parallelism operator parallelism
	 * @return this
	 */
	public VertexOutDegree<K,VV,EV> setParallelism(int parallelism) {
		this.parallelism = parallelism;

		return this;
	}

	@Override
	protected void hashCodeInternal(HashCodeBuilder builder) {
		builder.append(includeZeroDegreeVertices);
	}

	@Override
	protected void equalsInternal(EqualsBuilder builder, CachingGraphAlgorithm obj) {
		if (! VertexOutDegree.class.isAssignableFrom(obj.getClass())) {
			builder.appendSuper(false);
		}

		VertexOutDegree rhs = (VertexOutDegree) obj;

		builder.append(includeZeroDegreeVertices, rhs.includeZeroDegreeVertices);
	}

	@Override
	protected String getAlgorithmName() {
		return VertexOutDegree.class.getCanonicalName();
	}

	@Override
	public DataSet<Vertex<K,LongValue>> runInternal(Graph<K,VV,EV> input)
			throws Exception {
		// s
		DataSet<Vertex<K,LongValue>> sourceLabels = input
			.getEdges()
			.map(new MapEdgeToSourceLabel<K,EV>())
				.setParallelism(parallelism)
				.name("Map edge to source label");

		// s, deg(s)
		DataSet<Vertex<K,LongValue>> sourceDegree = sourceLabels
			.groupBy(0)
			.reduce(new DegreeCount<K>())
				.setParallelism(parallelism)
				.name("Degree count");

		if (includeZeroDegreeVertices) {
			sourceDegree = input
				.getVertices()
				.leftOuterJoin(sourceDegree)
				.where(0)
				.equalTo(0)
				.with(new JoinVertexWithVertexDegree<K,VV>())
					.setParallelism(parallelism)
					.name("Join zero degree vertices");
		}

		return sourceDegree;
	}
}
