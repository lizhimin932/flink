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

package org.apache.flink.fs.gcs.common;

import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.fs.gcs.common.writer.GcsRecoverableWriter;
import org.apache.flink.runtime.fs.hdfs.HadoopFileSystem;

import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * HadoopFileSystem extension for a {@link FlinkGcsFileSystem}.
 */
public class FlinkGcsFileSystem extends HadoopFileSystem {
	private static final Logger LOG = LoggerFactory.getLogger(FlinkGcsFileSystem.class);

	// ------------------- Recoverable Writer Parameters -------------------

	/** The minimum size of a part in the multipart upload, except for the last part: 64 MIBytes. */
	public static final long GCS_MULTIPART_MIN_PART_SIZE = 64L << 20;

	private final FileSystem hadoopFileSystem;

	FlinkGcsFileSystem(FileSystem hadoopFileSystem) throws IOException {
		super(hadoopFileSystem);

		this.hadoopFileSystem = hadoopFileSystem;
	}

	@Override
	public RecoverableWriter createRecoverableWriter() {
		return new GcsRecoverableWriter(this.hadoopFileSystem);
	}

}
