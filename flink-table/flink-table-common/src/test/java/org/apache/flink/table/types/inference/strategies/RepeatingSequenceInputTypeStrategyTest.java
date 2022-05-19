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

package org.apache.flink.table.types.inference.strategies;

import org.apache.flink.table.types.inference.InputTypeStrategiesTestBase;

import java.util.stream.Stream;

import static org.apache.flink.table.api.DataTypes.INT;
import static org.apache.flink.table.api.DataTypes.STRING;
import static org.apache.flink.table.types.inference.InputTypeStrategies.explicit;
import static org.apache.flink.table.types.inference.InputTypeStrategies.repeatingSequence;

/** Tests for {@link RepeatingSequenceInputTypeStrategy}. */
class RepeatingSequenceInputTypeStrategyTest extends InputTypeStrategiesTestBase {

    protected Stream<TestSpec> testData() {
        return Stream.of(
                TestSpec.forStrategy(
                                "Single occurrence",
                                repeatingSequence(explicit(INT()), explicit(STRING())))
                        .calledWithArgumentTypes(INT(), STRING())
                        .expectArgumentTypes(INT(), STRING()),
                TestSpec.forStrategy(
                                "Multiple occurrences",
                                repeatingSequence(explicit(INT()), explicit(STRING())))
                        .calledWithArgumentTypes(INT(), STRING(), INT(), STRING(), INT(), STRING())
                        .expectArgumentTypes(INT(), STRING(), INT(), STRING(), INT(), STRING()),
                TestSpec.forStrategy(
                                "Incorrect order of sequence",
                                repeatingSequence(explicit(INT()), explicit(STRING())))
                        .calledWithArgumentTypes(STRING(), INT())
                        .expectErrorMessage(
                                String.format(
                                        "Invalid input arguments. Expected signatures are:%nf([INT, STRING]...)")),
                TestSpec.forStrategy(
                                "Incorrect number of arguments",
                                repeatingSequence(explicit(INT()), explicit(STRING())))
                        .calledWithArgumentTypes(INT())
                        .expectErrorMessage(
                                String.format(
                                        "Invalid input arguments. Expected signatures are:%nf([INT, STRING]...)")));
    }
}
