/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;

/**
 * Utility for copying from a HDFS {@link FileSystem} to the local file system in an external
 * process. This is required since {@code FileSystem.copyToLocalFile} does not like being
 * interrupted.
 */
public class HDFSCopyToLocal {
	public static void main(String[] args) throws Exception {
		String backupUri = args[0];
		String dbPath = args[1];

		FileSystem fs = FileSystem.get(new URI(backupUri), new Configuration());

		fs.copyToLocalFile(new Path(backupUri), new Path(dbPath));
	}

	public static void copyToLocal(URI remotePath, File localPath) throws Exception {
		ExternalProcessRunner processRunner = new ExternalProcessRunner(HDFSCopyToLocal.class.getName(),
			new String[]{remotePath.toString(), localPath.getAbsolutePath()});
		if (processRunner.run() != 0) {
			throw new  RuntimeException("Error while copying from remote FileSystem: " + processRunner.getErrorOutput());
		}
	}
}
