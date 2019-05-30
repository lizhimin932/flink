/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.ml.params.outlier;

import org.apache.flink.ml.params.BaseWithParam;
import org.apache.flink.ml.params.ParamInfo;
import org.apache.flink.ml.params.shared.colname.HasPredResultColName;
import org.apache.flink.ml.params.shared.colname.HasVectorColName;

/**
 * Params of SOS.
 */
public interface SosParams<T> extends
	BaseWithParam <T>,
	HasVectorColName <T>,
	HasPredResultColName <T> {

	/**
	 * @cn Perplexity
	 */
	ParamInfo <Double> PERPLEXITY = new ParamInfo <>(
		"perplexity",
		"Perplexity",
		true, 4.0,
		Double.class
	);

	default Double getPerplexity() {
		return getParams().get(PERPLEXITY);
	}

	default T setPerplexity(Double value) {
		return set(PERPLEXITY, value);
	}

}
