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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;

/**
 * Udf stream operator factory which just wrap existed {@link AbstractUdfStreamOperator}.
 *
 * @param <OUT> The output type of the operator
 */
@Internal
public class SimpleUdfStreamOperatorFactory<OUT> extends SimpleOperatorFactory<OUT> implements UdfStreamOperatorFactory<OUT> {

	private final AbstractUdfStreamOperator<OUT, ?> operator;

	public SimpleUdfStreamOperatorFactory(AbstractUdfStreamOperator<OUT, ?> operator) {
		super(operator);
		this.operator = operator;
	}

	@Override
	public Function getUserFunction() {
		return operator.getUserFunction();
	}

	@Override
	public String getUserFunctionClassName() {
		return operator.getUserFunction().getClass().getName();
	}

}
