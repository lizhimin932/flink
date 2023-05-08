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

package org.apache.flink.connector.file.table;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableDescriptor;
import org.apache.flink.table.planner.runtime.utils.StreamingTestBaseV2;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test of the filesystem source in streaming mode. */
public class FileSystemTableSinkStreamingITCase extends StreamingTestBaseV2 {

    @TempDir private static File tempFolder;

    @Test
    public void testMonitorContinuously() throws Exception {
        // Create temp dir
        File testPath = tempFolder;

        // Write first csv file out
        Files.write(
                Paths.get(testPath.getPath(), "input_0.csv"),
                Arrays.asList("1", "2", "3"),
                StandardOpenOption.CREATE);

        Duration monitorInterval = Duration.ofSeconds(1);

        tEnv.createTable(
                "my_streaming_table",
                TableDescriptor.forConnector("filesystem")
                        .schema(Schema.newBuilder().column("data", DataTypes.INT()).build())
                        .format("testcsv")
                        .option(FileSystemConnectorOptions.PATH, testPath.getPath())
                        .option(FileSystemConnectorOptions.SOURCE_MONITOR_INTERVAL, monitorInterval)
                        .build());

        List<Integer> actual = new ArrayList<>();

        try (CloseableIterator<Row> resultsIterator =
                tEnv.sqlQuery("SELECT * FROM my_streaming_table").execute().collect()) {
            // Iterate over the first 3 rows
            for (int i = 0; i < 3; i++) {
                actual.add(resultsIterator.next().<Integer>getFieldAs(0));
            }

            // Write second csv file out
            Files.write(
                    Paths.get(testPath.getPath(), "input_1.csv"),
                    Arrays.asList("4", "5", "6"),
                    StandardOpenOption.CREATE);

            // Iterate over the next 3 rows
            for (int i = 0; i < 3; i++) {
                actual.add(resultsIterator.next().<Integer>getFieldAs(0));
            }
        }

        assertThat(actual).containsExactlyInAnyOrder(1, 2, 3, 4, 5, 6);
    }
}
