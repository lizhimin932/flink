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
package org.apache.flink.table.plan.nodes.datastream.function;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlKind;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.summarize.aggregation.Aggregator;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class DataStreamProcTimeAggregateRowWindowFunction
		implements WindowFunction<Object, Object, Tuple, GlobalWindow> {

	static final long serialVersionUID = 1L;
	List<String> aggregators;
	List<Integer> indexes;
	List<TypeInformation<?>> typeInfo;
	@SuppressWarnings("rawtypes")
	Map<String, List<Aggregator>> aggregatorImpl;

	public DataStreamProcTimeAggregateRowWindowFunction(List<String> aggregators, List<Integer> rowIndexes,
			List<TypeInformation<?>> typeInfos) {
		this.aggregators = aggregators;
		this.indexes = rowIndexes;
		this.typeInfo = typeInfos;
		aggregatorImpl = new HashMap<>();
	}

	Row reuse;
	Row result;

	@SuppressWarnings("unchecked")
	@Override
	public void apply(Tuple key, GlobalWindow window, Iterable<Object> input, Collector<Object> out) throws Exception {
		
		String keyString = key.toString();
		for (Object rowObj : input) {
			reuse = (Row) rowObj;
			if (result == null) {
				result = new Row(reuse.getArity() + aggregators.size());
			}
			for (int i = 0; i < aggregators.size(); i++) {
				setAggregator(key, i, aggregators.get(i));
				aggregatorImpl.get(keyString).get(i).aggregate(reuse.getField(indexes.get(i)));
			}
		}
		
		for (int i = 0; i < reuse.getArity(); i++) {
			result.setField(i, reuse.getField(i));
		}
		for (int i = 0; i < aggregators.size(); i++) {
			result.setField(reuse.getArity() + i, aggregatorImpl.get(keyString).get(i).result());
		}

		out.collect(result);
	}

	@SuppressWarnings("rawtypes")
	private void setAggregator(Tuple key, int i, String aggregatorName) {
		if (typeInfo.get(i).getTypeClass().equals(Integer.class)) {
			if (!aggregatorImpl.containsKey(key.toString())) {
				List<Aggregator> aggs = new ArrayList<>();
				aggs.add(getIntegerAggregator(aggregatorName));
				aggregatorImpl.put(key.toString(), aggs);
			}
			if (aggregatorImpl.get(key.toString()).size() - 1 < i) {
				aggregatorImpl.get(key.toString()).add(getIntegerAggregator(aggregatorName));
			}
		} else if (typeInfo.get(i).getTypeClass().equals(Double.class)) {
			if (!aggregatorImpl.containsKey(key.toString())) {
				List<Aggregator> aggs = new ArrayList<>();
				aggs.add(getDoubleAggregator(aggregatorName));
				aggregatorImpl.put(key.toString(), aggs);
			}
			if (aggregatorImpl.get(key.toString()).size() - 1 < i) {
				aggregatorImpl.get(key.toString()).add(getDoubleAggregator(aggregatorName));
			}
		} else if (typeInfo.get(i).getTypeClass().equals(Long.class)) {
			if (!aggregatorImpl.containsKey(key.toString())) {
				List<Aggregator> aggs = new ArrayList<>();
				aggs.add(getLongAggregator(aggregatorName));
				aggregatorImpl.put(key.toString(), aggs);
			}
			if (aggregatorImpl.get(key.toString()).size() - 1 < i) {
				aggregatorImpl.get(key.toString()).add(getLongAggregator(aggregatorName));
			}

		} else {
			throw new IllegalArgumentException(
					"Unsupported aggregation type for MAX. Only Integer, Double, Long supported.");
		}
	}

	private Aggregator<Integer, Integer> getIntegerAggregator(String aggregatorName) {
		Aggregator<Integer, Integer> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new IntegerSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new IntegerSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString())) {
			aggregator = new IntegerSummaryAggregation().initSum();
		} else {
			throw new IllegalArgumentException("Unsupported aggregation type of aggregation: " + aggregatorName
					+ ". Only MAX, MIN, SUM supported.");
		}
		return aggregator;
	}

	private Aggregator<Double, Double> getDoubleAggregator(String aggregatorName) {
		Aggregator<Double, Double> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new DoubleSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new DoubleSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString())) {
			aggregator = new DoubleSummaryAggregation().initSum();
		} else {
			throw new IllegalArgumentException(
					"Unsupported aggregation type for " + aggregatorName + ". Only Integer, Double, Long supported.");
		}
		return aggregator;
	}

	private Aggregator<Long, Long> getLongAggregator(String aggregatorName) {
		Aggregator<Long, Long> aggregator = null;
		if (aggregatorName.equals(SqlKind.MAX.toString())) {
			aggregator = new LongSummaryAggregation().initMax();
		} else if (aggregatorName.equals(SqlKind.MIN.toString())) {
			aggregator = new LongSummaryAggregation().initMin();
		} else if (aggregatorName.equals(SqlKind.SUM.toString())) {
			aggregator = new LongSummaryAggregation().initSum();
		} else {
			throw new IllegalArgumentException(
					"Unsupported aggregation type for " + aggregatorName + ". Only Integer, Double, Long supported.");
		}
		return aggregator;
	}
}
