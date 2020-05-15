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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.operators.util.OperatorValidationUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.SourceOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;

/**
 * The DataStreamSource represents the starting point of a DataStream.
 *
 * @param <T> Type of the elements in the DataStream created from the this source.
 */
@Public
public class DataStreamSource<T> extends SingleOutputStreamOperator<T> {

	boolean isParallel;

	public DataStreamSource(StreamExecutionEnvironment environment,
			TypeInformation<T> outTypeInfo, StreamSource<T, ?> operator,
			boolean isParallel, String sourceName) {
		super(environment, new LegacySourceTransformation<>(sourceName, operator, outTypeInfo, environment.getParallelism()));

		this.isParallel = isParallel;
		if (!isParallel) {
			setParallelism(1);
		}
	}

	public DataStreamSource(SingleOutputStreamOperator<T> operator) {
		super(operator.environment, operator.getTransformation());
		this.isParallel = true;
	}

	public DataStreamSource(
			StreamExecutionEnvironment environment,
			Source<T, ?, ?> source,
			TypeInformation<T> outTypeInfo,
			String sourceName) {
		super(environment,
				new LegacySourceTransformation<>(
						sourceName,
						new SourceOperatorFactory<>(source),
						outTypeInfo,
						environment.getParallelism()));
	}

	@Override
	public DataStreamSource<T> setParallelism(int parallelism) {
		OperatorValidationUtils.validateParallelism(parallelism, isParallel);
		super.setParallelism(parallelism);
		return this;
	}
}
