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

package org.apache.flink.testutils.junit.utils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;

/** The utils contains some methods same as {@link org.junit.rules.TemporaryFolder}. */
public class TempDirUtils {
    private static final String TMP_PREFIX = "junit";

    public static File newFolder(Path path) throws IOException {
        Path tempPath;
        if (path != null) {
            tempPath = Files.createTempDirectory(path, TMP_PREFIX, new FileAttribute[0]);
        } else {
            tempPath = Files.createTempDirectory(TMP_PREFIX, new FileAttribute[0]);
        }
        return tempPath.toFile();
    }

    public static File newFile(Path path) throws IOException {
        return File.createTempFile(TMP_PREFIX, null, path.toFile());
    }

    public static File newFolder(Path base, String... paths) throws IOException {
        if (paths.length == 0) {
            throw new IllegalArgumentException("must pass at least one path");
        }

        /*
         * Before checking if the paths are absolute paths, check if create() was ever called,
         * and if it wasn't, throw IllegalStateException.
         */
        File root = base.toFile();
        for (String path : paths) {
            if (new File(path).isAbsolute()) {
                throw new IOException("folder path \'" + path + "\' is not a relative path");
            }
        }

        File relativePath = null;
        File file = root;
        boolean lastMkdirsCallSuccessful = true;
        for (String path : paths) {
            relativePath = new File(relativePath, path);
            file = new File(root, relativePath.getPath());

            lastMkdirsCallSuccessful = file.mkdirs();
            if (!lastMkdirsCallSuccessful && !file.isDirectory()) {
                if (file.exists()) {
                    throw new IOException(
                            "a file with the path \'" + relativePath.getPath() + "\' exists");
                } else {
                    throw new IOException(
                            "could not create a folder with the path \'"
                                    + relativePath.getPath()
                                    + "\'");
                }
            }
        }
        if (!lastMkdirsCallSuccessful) {
            throw new IOException(
                    "a folder with the path \'" + relativePath.getPath() + "\' already exists");
        }
        return file;
    }

    public static File newFile(Path folder, String fileName) throws IOException {
        File file = new File(folder.toFile(), fileName);
        if (!file.createNewFile()) {
            throw new IOException(
                    "a file with the name \'" + fileName + "\' already exists in the test folder");
        }
        return file;
    }
}
