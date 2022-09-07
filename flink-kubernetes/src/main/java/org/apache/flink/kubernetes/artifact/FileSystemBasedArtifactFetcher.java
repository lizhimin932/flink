/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.kubernetes.artifact;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** Leverage the flink filesystem plugin to fetch the artifact. */
public class FileSystemBasedArtifactFetcher implements ArtifactFetcher {

    public static final Logger LOG = LoggerFactory.getLogger(FileSystemBasedArtifactFetcher.class);
    public static final FileSystemBasedArtifactFetcher INSTANCE =
            new FileSystemBasedArtifactFetcher();

    @Override
    public File fetch(String uri, Configuration flinkConfiguration, File targetDir)
            throws Exception {
        org.apache.flink.core.fs.Path source = new org.apache.flink.core.fs.Path(uri);
        long start = System.currentTimeMillis();
        FileSystem fileSystem = source.getFileSystem();
        String fileName = source.getName();
        File targetFile = new File(targetDir, fileName);
        try (FSDataInputStream inputStream = fileSystem.open(source)) {
            FileUtils.copyToFile(inputStream, targetFile);
        }
        LOG.debug(
                "Copied file from {} to {}, cost {} ms",
                source,
                targetFile,
                System.currentTimeMillis() - start);
        return targetFile;
    }
}
