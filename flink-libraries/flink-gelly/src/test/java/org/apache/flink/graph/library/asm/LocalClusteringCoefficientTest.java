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

import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.Utils.ChecksumHashCode;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.graph.asm.AsmTestBase;
import org.apache.flink.graph.library.asm.LocalClusteringCoefficient.Result;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocalClusteringCoefficientTest
extends AsmTestBase {

	@Test
	public void testSimpleGraph()
			throws Exception {
		DataSet<Result<IntValue>> cc = undirectedSimpleGraph
			.run(new LocalClusteringCoefficient<IntValue, NullValue, NullValue>());

		String expectedResult =
			"(0,(2,1))\n" +
			"(1,(3,2))\n" +
			"(2,(3,2))\n" +
			"(3,(4,1))\n" +
			"(4,(1,0))\n" +
			"(5,(1,0))";

		TestBaseUtils.compareResultAsText(cc.collect(), expectedResult);
	}

	@Test
	public void testCompleteGraph()
			throws Exception {
		long expectedDegree = completeGraphVertexCount - 1;
		long count = CombinatoricsUtils.binomialCoefficient((int)expectedDegree, 2);

		DataSet<Result<LongValue>> cc = completeGraph
			.run(new LocalClusteringCoefficient<LongValue, NullValue, NullValue>());

		for (Result<LongValue> result : cc.collect()) {
			assertEquals(expectedDegree, result.getDegree().getValue());
			assertEquals(count, result.getTriangleCount().getValue());
		}
	}

	@Test
	public void testRMatGraph()
			throws Exception {
		DataSet<Result<LongValue>> cc = undirectedRMatGraph
			.run(new LocalClusteringCoefficient<LongValue, NullValue, NullValue>());

		ChecksumHashCode checksum = DataSetUtils.checksumHashCode(cc);

		assertEquals(902, checksum.getCount());
		assertEquals(0x000001b08e783277L, checksum.getChecksum());
	}
}
