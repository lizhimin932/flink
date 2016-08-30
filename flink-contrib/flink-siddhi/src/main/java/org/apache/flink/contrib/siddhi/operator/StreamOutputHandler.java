/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.siddhi.operator;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.contrib.siddhi.utils.TupleUtils;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;

import java.util.HashMap;
import java.util.Map;

public class StreamOutputHandler<R> extends StreamCallback {
	private static final Logger LOGGER = LoggerFactory.getLogger(StreamOutputHandler.class);

	private final AbstractDefinition definition;
	private final Output<StreamRecord<R>> output;
	private final TypeInformation<R> typeInfo;
	private final ObjectMapper objectMapper;

	public StreamOutputHandler(TypeInformation<R> typeInfo, AbstractDefinition definition, Output<StreamRecord<R>> output) {
		this.typeInfo = typeInfo;
		this.definition = definition;
		this.output = output;
		this.objectMapper = new ObjectMapper();
		this.objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void receive(Event[] events) {
		for (Event event : events) {
			if (typeInfo == null || typeInfo.getTypeClass().equals(Map.class)) {
				StreamRecord<R> record = new StreamRecord<>((R) toMap(event));
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else if (typeInfo.isTupleType()) {
				StreamRecord<R> record = new StreamRecord<>((R) toTuple(event));
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else if (typeInfo instanceof PojoTypeInfo) {
				R obj;
				try {
					obj = objectMapper.convertValue(toMap(event), typeInfo.getTypeClass());
				} catch (IllegalArgumentException ex) {
					LOGGER.error("Failed to map event: " + event + " into type: " + typeInfo, ex);
					throw ex;
				}
				StreamRecord<R> record = new StreamRecord<>(obj);
				record.setTimestamp(event.getTimestamp());
				output.collect(record);
			} else {
				throw new IllegalArgumentException("Unable to format " + event + " as type " + typeInfo);
			}
		}
		output.collect(new StreamRecord<R>(null));
	}

	private Map<String, Object> toMap(Event event) {
		Map<String, Object> map = new HashMap<>();
		for (int i = 0; i < definition.getAttributeNameArray().length; i++) {
			map.put(definition.getAttributeNameArray()[i], event.getData(i));
		}
		return map;
	}

	private Tuple toTuple(Event event) {
		return TupleUtils.newTuple(event.getData());
	}
}
