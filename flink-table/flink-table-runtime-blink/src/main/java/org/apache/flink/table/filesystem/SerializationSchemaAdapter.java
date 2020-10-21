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

package org.apache.flink.table.filesystem;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.table.data.RowData;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;

/**
 * Adapter to turn a {@link SerializationSchema} into a {@link Encoder}.
 */
@Internal
public class SerializationSchemaAdapter implements Encoder<RowData> {

	private static final long serialVersionUID = 1L;

	static final byte LINE_DELIMITER = "\n".getBytes(StandardCharsets.UTF_8)[0];

	private final SerializationSchema<RowData> serializationSchema;

	public SerializationSchemaAdapter(SerializationSchema<RowData> serializationSchema) {
		this.serializationSchema = serializationSchema;
	}

	@Override
	public void encode(RowData element, OutputStream stream) throws IOException {
		stream.write(serializationSchema.serialize(element));
		stream.write(LINE_DELIMITER);
	}
}
