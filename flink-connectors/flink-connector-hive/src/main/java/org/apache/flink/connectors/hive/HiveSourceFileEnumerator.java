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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.connectors.hive.read.HiveSourceSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.util.Preconditions;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link FileEnumerator} implementation for hive source, which generates splits based on {@link
 * HiveTablePartition}s.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {

    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;
    private final JobConf jobConf;

    public HiveSourceFileEnumerator(List<HiveTablePartition> partitions, JobConf jobConf) {
        this.partitions = partitions;
        this.jobConf = jobConf;
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(createInputSplits(minDesiredSplits, partitions, jobConf, false));
    }

    public static List<HiveSourceSplit> createInputSplits(
            int minNumSplits,
            List<HiveTablePartition> partitions,
            JobConf jobConf,
            boolean isForParallelismInfer)
            throws IOException {
        if (isForParallelismInfer) {
            // it's for parallelism inference, we will try to use the configuration
            // "table.exec.hive.infer-source-parallelism.max" as the min split num
            // to split the Hive files to improve the parallelism
            setSplitMaxSize(
                    partitions,
                    jobConf,
                    Integer.parseInt(
                            jobConf.get(
                                    HiveOptions.TABLE_EXEC_HIVE_INFER_SOURCE_PARALLELISM_MAX
                                            .key())));

        } else {
            setSplitMaxSize(partitions, jobConf, minNumSplits);
        }
        int threadNum = getThreadNumToSplitHiveFile(jobConf);
        Preconditions.checkArgument(
                threadNum >= 1,
                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key()
                        + " cannot be less than 1");
        List<HiveSourceSplit> hiveSplits = new ArrayList<>();
        try (MRSplitsGetter splitsGetter = new MRSplitsGetter(threadNum)) {
            for (HiveTablePartitionSplits partitionSplits :
                    splitsGetter.getHiveTablePartitionMRSplits(minNumSplits, partitions, jobConf)) {
                HiveTablePartition partition = partitionSplits.getHiveTablePartition();
                for (InputSplit inputSplit : partitionSplits.getInputSplits()) {
                    Preconditions.checkState(
                            inputSplit instanceof FileSplit,
                            "Unsupported InputSplit type: " + inputSplit.getClass().getName());
                    hiveSplits.add(new HiveSourceSplit((FileSplit) inputSplit, partition, null));
                }
            }
        }
        return hiveSplits;
    }

    private static boolean supportSetSplitMaxSize(List<HiveTablePartition> partitions) {
        // now, the configuration 'HiveConf.ConfVars.MAPREDMAXSPLITSIZE' we set only
        // works for orc format
        for (HiveTablePartition partition : partitions) {
            String serializationLib =
                    partition.getStorageDescriptor().getSerdeInfo().getSerializationLib();
            if (!"orc".equalsIgnoreCase(serializationLib)) {
                return false;
            }
        }
        return !partitions.isEmpty();
    }

    private static void setSplitMaxSize(
            List<HiveTablePartition> partitions, JobConf jobConf, int minNumSplits)
            throws IOException {
        if (!supportSetSplitMaxSize(partitions)) {
            return;
        }
        // if minNumSplits <= 0, we set it to 1 manually
        minNumSplits = minNumSplits <= 0 ? 1 : minNumSplits;
        long defaultMaxSplitBytes = getSplitMaxSize(jobConf);
        long openCost = getFileOpenCost(jobConf);
        long totalByteWithOpenCost = calculateFilesSizeWithOpenCost(partitions, jobConf, openCost);
        long maxSplitBytes =
                calculateMaxSplitBytes(
                        totalByteWithOpenCost, minNumSplits, defaultMaxSplitBytes, openCost);
        jobConf.set(HiveConf.ConfVars.MAPREDMAXSPLITSIZE.varname, String.valueOf(maxSplitBytes));
    }

    private static long calculateMaxSplitBytes(
            long totalBytesWithWeight,
            int minNumSplits,
            long defaultMaxSplitBytes,
            long openCostInBytes) {
        long bytesPerSplit = totalBytesWithWeight / minNumSplits;
        return Math.min(defaultMaxSplitBytes, Math.max(openCostInBytes, bytesPerSplit));
    }

    private static long calculateFilesSizeWithOpenCost(
            List<HiveTablePartition> partitions, JobConf jobConf, long openCost)
            throws IOException {
        long totalBytesWithWeight = 0;
        for (HiveTablePartition partition : partitions) {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                continue;
            }
            for (FileStatus fileStatus : fs.listStatus(inputPath)) {
                long fileByte = fileStatus.getLen();
                totalBytesWithWeight += (fileByte + openCost);
            }
        }
        return totalBytesWithWeight;
    }

    public static int getNumFiles(List<HiveTablePartition> partitions, JobConf jobConf)
            throws IOException {
        int numFiles = 0;
        for (HiveTablePartition partition : partitions) {
            StorageDescriptor sd = partition.getStorageDescriptor();
            org.apache.hadoop.fs.Path inputPath = new org.apache.hadoop.fs.Path(sd.getLocation());
            FileSystem fs = inputPath.getFileSystem(jobConf);
            // it's possible a partition exists in metastore but the data has been removed
            if (!fs.exists(inputPath)) {
                continue;
            }
            numFiles += fs.listStatus(inputPath).length;
        }
        return numFiles;
    }

    private static long getSplitMaxSize(JobConf jobConf) {
        return jobConf.getLong(
                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.key(),
                HiveOptions.TABLE_EXEC_HIVE_SPLIT_MAX_BYTES.defaultValue().getBytes());
    }

    private static long getFileOpenCost(JobConf jobConf) {
        return jobConf.getLong(
                HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.key(),
                HiveOptions.TABLE_EXEC_HIVE_FILE_OPEN_COST.defaultValue().getBytes());
    }

    private static int getThreadNumToSplitHiveFile(JobConf jobConf) {
        return jobConf.getInt(
                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.key(),
                HiveOptions.TABLE_EXEC_HIVE_LOAD_PARTITION_SPLITS_THREAD_NUM.defaultValue());
    }

    /** A factory to create {@link HiveSourceFileEnumerator}. */
    public static class Provider implements FileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final List<HiveTablePartition> partitions;
        private final JobConfWrapper jobConfWrapper;

        public Provider(List<HiveTablePartition> partitions, JobConfWrapper jobConfWrapper) {
            this.partitions = partitions;
            this.jobConfWrapper = jobConfWrapper;
        }

        @Override
        public FileEnumerator create() {
            return new HiveSourceFileEnumerator(partitions, jobConfWrapper.conf());
        }
    }
}
