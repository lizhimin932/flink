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

package org.apache.flink.python.util;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;

import java.util.Random;

/**
 * A custom source function to for testing add a custom source in Python StreamExecutionEnvironment.
 */
public class MyCustomSourceFunction implements SourceFunction<Row> {

	private static final String[] NAMES = {"Bob", "Marry", "Henry", "Mike", "Ted", "Jack"};

	private int recordCount = 50;

	public MyCustomSourceFunction() {
	}

	public MyCustomSourceFunction(int recordCount) {
		this.recordCount = recordCount;
	}

	public void run(SourceContext sourceContext) {
		Random random = new Random();
		for (int i = 0; i < recordCount; i++) {
			Row row = Row.of(random.nextInt(1000), NAMES[random.nextInt(NAMES.length)], random.nextDouble());
			sourceContext.collect(row);
		}

	}

	public void cancel() {
	}
}
