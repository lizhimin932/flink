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

package org.apache.flink.optimizer.programs;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.*;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("serial")
public class ConnectedComponentsTest extends CompilerTestBase {

    private static final String VERTEX_SOURCE = "Vertices";

    private static final String ITERATION_NAME = "Connected Components Iteration";

    private static final String EDGES_SOURCE = "Edges";
    private static final String JOIN_NEIGHBORS_MATCH = "Join Candidate Id With Neighbor";
    private static final String MIN_ID_REDUCER = "Find Minimum Candidate Id";
    private static final String UPDATE_ID_MATCH = "Update Component Id";

    private static final String SINK = "Result";

    private final FieldList set0 = new FieldList(0);

    @Test
    public void testWorksetConnectedComponents() {
        Plan plan = getConnectedComponentsPlan(DEFAULT_PARALLELISM, 100, false);

        OptimizedPlan optPlan = compileNoStats(plan);
        OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);

        SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
        SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
        SinkPlanNode sink = or.getNode(SINK);
        WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);

        DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
        SingleInputPlanNode minIdReducer = or.getNode(MIN_ID_REDUCER);
        SingleInputPlanNode minIdCombiner = (SingleInputPlanNode) minIdReducer.getPredecessor();
        DualInputPlanNode updatingMatch = or.getNode(UPDATE_ID_MATCH);

        // test all drivers
        Assertions.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());

        Assertions.assertEquals(
                DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, neighborsJoin.getDriverStrategy());
        Assertions.assertTrue(!neighborsJoin.getInput1().getTempMode().isCached());
        Assertions.assertTrue(!neighborsJoin.getInput2().getTempMode().isCached());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput1());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput2());

        Assertions.assertEquals(
                DriverStrategy.HYBRIDHASH_BUILD_SECOND, updatingMatch.getDriverStrategy());
        Assertions.assertEquals(set0, updatingMatch.getKeysForInput1());
        Assertions.assertEquals(set0, updatingMatch.getKeysForInput2());

        // test all the shipping strategies
        Assertions.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                iter.getInitialSolutionSetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());

        Assertions.assertEquals(
                ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                neighborsJoin.getInput2().getShipStrategy()); // edges
        Assertions.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());

        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, minIdReducer.getInput().getShipStrategy());
        Assertions.assertEquals(set0, minIdReducer.getInput().getShipStrategyKeys());
        Assertions.assertEquals(
                ShipStrategyType.FORWARD, minIdCombiner.getInput().getShipStrategy());

        Assertions.assertEquals(
                ShipStrategyType.FORWARD, updatingMatch.getInput1().getShipStrategy()); // min id
        Assertions.assertEquals(
                ShipStrategyType.FORWARD,
                updatingMatch.getInput2().getShipStrategy()); // solution set

        // test all the local strategies
        Assertions.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, iter.getInitialWorksetInput().getLocalStrategy());

        Assertions.assertEquals(
                LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
        Assertions.assertEquals(
                LocalStrategy.NONE, neighborsJoin.getInput2().getLocalStrategy()); // edges

        Assertions.assertEquals(
                LocalStrategy.COMBININGSORT, minIdReducer.getInput().getLocalStrategy());
        Assertions.assertEquals(set0, minIdReducer.getInput().getLocalStrategyKeys());
        Assertions.assertEquals(LocalStrategy.NONE, minIdCombiner.getInput().getLocalStrategy());

        Assertions.assertEquals(
                LocalStrategy.NONE, updatingMatch.getInput1().getLocalStrategy()); // min id
        Assertions.assertEquals(
                LocalStrategy.NONE, updatingMatch.getInput2().getLocalStrategy()); // solution set

        // check the dams
        Assertions.assertEquals(TempMode.NONE, iter.getInitialWorksetInput().getTempMode());
        Assertions.assertEquals(TempMode.NONE, iter.getInitialSolutionSetInput().getTempMode());

        Assertions.assertEquals(
                DataExchangeMode.BATCH, iter.getInitialWorksetInput().getDataExchangeMode());
        Assertions.assertEquals(
                DataExchangeMode.BATCH, iter.getInitialSolutionSetInput().getDataExchangeMode());

        JobGraphGenerator jgg = new JobGraphGenerator();
        jgg.compileJobGraph(optPlan);
    }

    @Test
    public void testWorksetConnectedComponentsWithSolutionSetAsFirstInput() {

        Plan plan = getConnectedComponentsPlan(DEFAULT_PARALLELISM, 100, true);

        OptimizedPlan optPlan = compileNoStats(plan);
        OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);

        SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
        SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
        SinkPlanNode sink = or.getNode(SINK);
        WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);

        DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
        SingleInputPlanNode minIdReducer = or.getNode(MIN_ID_REDUCER);
        SingleInputPlanNode minIdCombiner = (SingleInputPlanNode) minIdReducer.getPredecessor();
        DualInputPlanNode updatingMatch = or.getNode(UPDATE_ID_MATCH);

        // test all drivers
        Assertions.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
        Assertions.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());

        Assertions.assertEquals(
                DriverStrategy.HYBRIDHASH_BUILD_SECOND_CACHED, neighborsJoin.getDriverStrategy());
        Assertions.assertTrue(!neighborsJoin.getInput1().getTempMode().isCached());
        Assertions.assertTrue(!neighborsJoin.getInput2().getTempMode().isCached());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput1());
        Assertions.assertEquals(set0, neighborsJoin.getKeysForInput2());

        Assertions.assertEquals(
                DriverStrategy.HYBRIDHASH_BUILD_FIRST, updatingMatch.getDriverStrategy());
        Assertions.assertEquals(set0, updatingMatch.getKeysForInput1());
        Assertions.assertEquals(set0, updatingMatch.getKeysForInput2());

        // test all the shipping strategies
        Assertions.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                iter.getInitialSolutionSetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
        Assertions.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());

        Assertions.assertEquals(
                ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH,
                neighborsJoin.getInput2().getShipStrategy()); // edges
        Assertions.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());

        Assertions.assertEquals(
                ShipStrategyType.PARTITION_HASH, minIdReducer.getInput().getShipStrategy());
        Assertions.assertEquals(set0, minIdReducer.getInput().getShipStrategyKeys());
        Assertions.assertEquals(
                ShipStrategyType.FORWARD, minIdCombiner.getInput().getShipStrategy());

        Assertions.assertEquals(
                ShipStrategyType.FORWARD,
                updatingMatch.getInput1().getShipStrategy()); // solution set
        Assertions.assertEquals(
                ShipStrategyType.FORWARD, updatingMatch.getInput2().getShipStrategy()); // min id

        // test all the local strategies
        Assertions.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());
        Assertions.assertEquals(
                LocalStrategy.NONE, iter.getInitialWorksetInput().getLocalStrategy());

        Assertions.assertEquals(
                LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
        Assertions.assertEquals(
                LocalStrategy.NONE, neighborsJoin.getInput2().getLocalStrategy()); // edges

        Assertions.assertEquals(
                LocalStrategy.COMBININGSORT, minIdReducer.getInput().getLocalStrategy());
        Assertions.assertEquals(set0, minIdReducer.getInput().getLocalStrategyKeys());
        Assertions.assertEquals(LocalStrategy.NONE, minIdCombiner.getInput().getLocalStrategy());

        Assertions.assertEquals(
                LocalStrategy.NONE, updatingMatch.getInput1().getLocalStrategy()); // min id
        Assertions.assertEquals(
                LocalStrategy.NONE, updatingMatch.getInput2().getLocalStrategy()); // solution set

        // check the dams
        Assertions.assertEquals(TempMode.NONE, iter.getInitialWorksetInput().getTempMode());
        Assertions.assertEquals(TempMode.NONE, iter.getInitialSolutionSetInput().getTempMode());

        Assertions.assertEquals(
                DataExchangeMode.BATCH, iter.getInitialWorksetInput().getDataExchangeMode());
        Assertions.assertEquals(
                DataExchangeMode.BATCH, iter.getInitialSolutionSetInput().getDataExchangeMode());

        JobGraphGenerator jgg = new JobGraphGenerator();
        jgg.compileJobGraph(optPlan);
    }

    private static Plan getConnectedComponentsPlan(
            int parallelism, int iterations, boolean solutionSetFirst) {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);

        DataSet<Tuple2<Long, Long>> verticesWithId =
                env.generateSequence(0, 1000)
                        .name("Vertices")
                        .map(
                                new MapFunction<Long, Tuple2<Long, Long>>() {
                                    @Override
                                    public Tuple2<Long, Long> map(Long value) {
                                        return new Tuple2<Long, Long>(value, value);
                                    }
                                })
                        .name("Assign Vertex Ids");

        DeltaIteration<Tuple2<Long, Long>, Tuple2<Long, Long>> iteration =
                verticesWithId
                        .iterateDelta(verticesWithId, iterations, 0)
                        .name("Connected Components Iteration");

        @SuppressWarnings("unchecked")
        DataSet<Tuple2<Long, Long>> edges =
                env.fromElements(new Tuple2<Long, Long>(0L, 0L)).name("Edges");

        DataSet<Tuple2<Long, Long>> minCandidateId =
                iteration
                        .getWorkset()
                        .join(edges)
                        .where(0)
                        .equalTo(0)
                        .projectSecond(1)
                        .<Tuple2<Long, Long>>projectFirst(1)
                        .name("Join Candidate Id With Neighbor")
                        .groupBy(0)
                        .min(1)
                        .name("Find Minimum Candidate Id");

        DataSet<Tuple2<Long, Long>> updateComponentId;

        if (solutionSetFirst) {
            updateComponentId =
                    iteration
                            .getSolutionSet()
                            .join(minCandidateId)
                            .where(0)
                            .equalTo(0)
                            .with(
                                    new FlatJoinFunction<
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>>() {
                                        @Override
                                        public void join(
                                                Tuple2<Long, Long> current,
                                                Tuple2<Long, Long> candidate,
                                                Collector<Tuple2<Long, Long>> out) {
                                            if (candidate.f1 < current.f1) {
                                                out.collect(candidate);
                                            }
                                        }
                                    })
                            .withForwardedFieldsFirst("0")
                            .withForwardedFieldsSecond("0")
                            .name("Update Component Id");
        } else {
            updateComponentId =
                    minCandidateId
                            .join(iteration.getSolutionSet())
                            .where(0)
                            .equalTo(0)
                            .with(
                                    new FlatJoinFunction<
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>,
                                            Tuple2<Long, Long>>() {
                                        @Override
                                        public void join(
                                                Tuple2<Long, Long> candidate,
                                                Tuple2<Long, Long> current,
                                                Collector<Tuple2<Long, Long>> out) {
                                            if (candidate.f1 < current.f1) {
                                                out.collect(candidate);
                                            }
                                        }
                                    })
                            .withForwardedFieldsFirst("0")
                            .withForwardedFieldsSecond("0")
                            .name("Update Component Id");
        }

        iteration
                .closeWith(updateComponentId, updateComponentId)
                .output(new DiscardingOutputFormat<Tuple2<Long, Long>>())
                .name("Result");

        return env.createProgramPlan();
    }
}
