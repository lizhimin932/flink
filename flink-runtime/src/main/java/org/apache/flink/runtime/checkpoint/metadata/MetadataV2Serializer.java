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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;

import java.io.DataInputStream;
import java.io.IOException;

/**
 * (De)serializer for checkpoint metadata format version 2.
 * This format was introduced with Apache Flink 1.3.0.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV2Serializer extends MetadataV2V3SerializerBase implements MetadataSerializer{

	/** The metadata format version. */
	public static final int VERSION = 2;

	/** The singleton instance of the serializer. */
	public static final MetadataV2Serializer INSTANCE = new MetadataV2Serializer();

	/** Singleton, not meant to be instantiated. */
	private MetadataV2Serializer() {}

	@Override
	public int getVersion() {
		return VERSION;
	}

	// ------------------------------------------------------------------------
	//  Deserialization entry point
	// ------------------------------------------------------------------------

	@Override
	public CheckpointMetadata deserialize(DataInputStream dis, ClassLoader classLoader) throws IOException {
		return deserializeMetadata(dis);
	}
}
