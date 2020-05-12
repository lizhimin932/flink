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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.data.util.DataFormatConverters.DataFormatConverter;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Lookup table function for Hive tables.
 */
public class HiveTableLookupFunction extends TableFunction<RowData> {

	private static final long serialVersionUID = 1L;

	private final HiveTableInputFormat inputFormat;
	// indices of lookup columns in the record returned by input format
	private final int[] lookupCols;
	// use Row as key because we'll get external data in eval
	private transient Map<Row, List<RowData>> cache;
	// timestamp when cache expires
	private transient long nextLoadTime;
	private final Duration cacheTTL;
	// serializer to copy RowData
	private transient TypeSerializer<RowData> serializer;
	// converters to convert data from internal to external in order to generate keys for the cache
	private final DataFormatConverter[] converters;

	public HiveTableLookupFunction(HiveTableInputFormat inputFormat, String[] lookupKeys, Duration cacheTTL) {
		lookupCols = new int[lookupKeys.length];
		converters = new DataFormatConverter[lookupKeys.length];
		String[] allFields = inputFormat.getFieldNames();
		Map<String, Integer> nameToIndex = IntStream.range(0, allFields.length).boxed().collect(
				Collectors.toMap(i -> allFields[i], i -> i));
		DataType[] allTypes = inputFormat.getFieldTypes();
		List<Integer> selectedIndices = Arrays.stream(inputFormat.getSelectedFields()).boxed().collect(Collectors.toList());
		for (int i = 0; i < lookupKeys.length; i++) {
			Integer index = nameToIndex.get(lookupKeys[i]);
			Preconditions.checkArgument(index != null, "Lookup keys %s not found in table schema", Arrays.toString(lookupKeys));
			converters[i] = DataFormatConverters.getConverterForDataType(allTypes[index]);
			index = selectedIndices.indexOf(index);
			Preconditions.checkArgument(index >= 0, "Lookup keys %s not selected", Arrays.toString(lookupKeys));
			lookupCols[i] = index;
		}
		this.inputFormat = inputFormat;
		this.cacheTTL = cacheTTL;
	}

	@Override
	public TypeInformation<RowData> getResultType() {
		String[] allNames = inputFormat.getFieldNames();
		DataType[] allTypes = inputFormat.getFieldTypes();
		int[] selected = inputFormat.getSelectedFields();
		return new RowDataTypeInfo(
				Arrays.stream(selected).mapToObj(i -> allTypes[i].getLogicalType()).toArray(LogicalType[]::new),
				Arrays.stream(selected).mapToObj(i -> allNames[i]).toArray(String[]::new));
	}

	@Override
	public void open(FunctionContext context) throws Exception {
		super.open(context);
		cache = new HashMap<>();
		nextLoadTime = -1;
		// TODO: get ExecutionConfig from context?
		serializer = getResultType().createSerializer(new ExecutionConfig());
	}

	public void eval(Object... values) {
		Preconditions.checkArgument(values.length == lookupCols.length, "Number of values and lookup keys mismatch");
		checkCacheReload();
		Row probeKey = Row.of(values);
		List<RowData> matchedRows = cache.get(probeKey);
		if (matchedRows != null) {
			for (RowData matchedRow : matchedRows) {
				collect(matchedRow);
			}
		}
	}

	@VisibleForTesting
	public Duration getCacheTTL() {
		return cacheTTL;
	}

	private void checkCacheReload() {
		if (nextLoadTime > System.currentTimeMillis()) {
			return;
		}
		cache.clear();
		try {
			HiveTableInputSplit[] inputSplits = inputFormat.createInputSplits(1);
			GenericRowData reuse = new GenericRowData(inputFormat.getSelectedFields().length);
			for (HiveTableInputSplit split : inputSplits) {
				inputFormat.open(split);
				while (!inputFormat.reachedEnd()) {
					RowData row = inputFormat.nextRecord(reuse);
					Row key = extractKey(row);
					List<RowData> rows = cache.computeIfAbsent(key, k -> new ArrayList<>());
					rows.add(serializer.copy(row));
				}
				inputFormat.close();
			}
			nextLoadTime = System.currentTimeMillis() + cacheTTL.toMillis();
		} catch (IOException e) {
			throw new FlinkHiveException("Failed to load hive table into cache", e);
		}
	}

	private Row extractKey(RowData row) {
		Row key = new Row(lookupCols.length);
		for (int i = 0; i < lookupCols.length; i++) {
			key.setField(i, converters[i].toExternal(row, lookupCols[i]));
		}
		return key;
	}
}
