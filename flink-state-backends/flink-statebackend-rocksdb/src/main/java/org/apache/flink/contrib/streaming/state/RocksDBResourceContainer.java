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

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.memory.OpaqueMemoryResource;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.BloomFilter;
import org.rocksdb.Cache;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Filter;
import org.rocksdb.IndexType;
import org.rocksdb.PlainTableConfig;
import org.rocksdb.ReadOptions;
import org.rocksdb.TableFormatConfig;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The container for RocksDB resources, including predefined options, option factory and shared
 * resource among instances.
 *
 * <p>This should be the only entrance for {@link EmbeddedRocksDBStateBackend} to get RocksDB
 * options, and should be properly (and necessarily) closed to prevent resource leak.
 */
public final class RocksDBResourceContainer implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBResourceContainer.class);

    /** The configurations from file. */
    private final ReadableConfig configuration;

    /** The pre-configured option settings. */
    private final PredefinedOptions predefinedOptions;

    /** The options factory to create the RocksDB options. */
    @Nullable private final RocksDBOptionsFactory optionsFactory;

    /**
     * The shared resource among RocksDB instances. This resource is not part of the
     * 'handlesToClose', because the handles to close are closed quietly, whereas for this one, we
     * want exceptions to be reported.
     */
    @Nullable private final OpaqueMemoryResource<RocksDBSharedResources> sharedResources;

    /** The handles to be closed when the container is closed. */
    private final ArrayList<AutoCloseable> handlesToClose;

    public RocksDBResourceContainer() {
        this(new Configuration(), PredefinedOptions.DEFAULT, null, null);
    }

    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions, @Nullable RocksDBOptionsFactory optionsFactory) {
        this(new Configuration(), predefinedOptions, optionsFactory, null);
    }

    public RocksDBResourceContainer(
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {
        this(new Configuration(), predefinedOptions, optionsFactory, sharedResources);
    }

    public RocksDBResourceContainer(
            ReadableConfig configuration,
            PredefinedOptions predefinedOptions,
            @Nullable RocksDBOptionsFactory optionsFactory,
            @Nullable OpaqueMemoryResource<RocksDBSharedResources> sharedResources) {

        this.configuration = configuration;
        this.predefinedOptions = checkNotNull(predefinedOptions);
        this.optionsFactory = optionsFactory;
        this.sharedResources = sharedResources;
        this.handlesToClose = new ArrayList<>();
    }

    /** Gets the RocksDB {@link DBOptions} to be used for RocksDB instances. */
    public DBOptions getDbOptions() {
        // initial options from common profile
        DBOptions opt = createBaseCommonDBOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setDBOptionsFromConfigurableOptions(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createDBOptions(opt, handlesToClose);
        }

        // add necessary default options
        opt = opt.setCreateIfMissing(true);

        // if sharedResources is non-null, use the write buffer manager from it.
        if (sharedResources != null) {
            opt.setWriteBufferManager(sharedResources.getResourceHandle().getWriteBufferManager());
        }

        return opt;
    }

    /**
     * Gets write buffer manager capacity.
     *
     * @return the capacity of the write buffer manager, or null if write buffer manager is not
     *     enabled.
     */
    public Long getWriteBufferManagerCapacity() {
        if (sharedResources == null) {
            return null;
        }

        return sharedResources.getResourceHandle().getWriteBufferManagerCapacity();
    }

    /** Gets the RocksDB {@link ColumnFamilyOptions} to be used for all RocksDB instances. */
    public ColumnFamilyOptions getColumnOptions() {
        // initial options from common profile
        ColumnFamilyOptions opt = createBaseCommonColumnOptions();
        handlesToClose.add(opt);

        // load configurable options on top of pre-defined profile
        setColumnFamilyOptionsFromConfigurableOptions(opt, handlesToClose);

        // add user-defined options, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createColumnOptions(opt, handlesToClose);
        }

        // if sharedResources is non-null, use the block cache from it and
        // set necessary options for performance consideration with memory control
        if (sharedResources != null) {
            final RocksDBSharedResources rocksResources = sharedResources.getResourceHandle();
            final Cache blockCache = rocksResources.getCache();
            TableFormatConfig tableFormatConfig = opt.tableFormatConfig();
            BlockBasedTableConfig blockBasedTableConfig;
            if (tableFormatConfig == null) {
                blockBasedTableConfig = new BlockBasedTableConfig();
            } else {
                Preconditions.checkArgument(
                        tableFormatConfig instanceof BlockBasedTableConfig,
                        "We currently only support BlockBasedTableConfig When bounding total memory.");
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
            if (rocksResources.isUsingPartitionedIndexFilters()
                    && overwriteFilterIfExist(blockBasedTableConfig)) {
                blockBasedTableConfig.setIndexType(IndexType.kTwoLevelIndexSearch);
                blockBasedTableConfig.setPartitionFilters(true);
                blockBasedTableConfig.setPinTopLevelIndexAndFilter(true);
            }
            blockBasedTableConfig.setBlockCache(blockCache);
            blockBasedTableConfig.setCacheIndexAndFilterBlocks(true);
            blockBasedTableConfig.setCacheIndexAndFilterBlocksWithHighPriority(true);
            blockBasedTableConfig.setPinL0FilterAndIndexBlocksInCache(true);
            opt.setTableFormatConfig(blockBasedTableConfig);
        }

        return opt;
    }

    /** Gets the RocksDB {@link WriteOptions} to be used for write operations. */
    public WriteOptions getWriteOptions() {
        // Disable WAL by default
        WriteOptions opt = new WriteOptions().setDisableWAL(true);
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createWriteOptions(opt, handlesToClose);
        }

        return opt;
    }

    /** Gets the RocksDB {@link ReadOptions} to be used for read operations. */
    public ReadOptions getReadOptions() {
        ReadOptions opt = new ReadOptions();
        handlesToClose.add(opt);

        // add user-defined options factory, if specified
        if (optionsFactory != null) {
            opt = optionsFactory.createReadOptions(opt, handlesToClose);
        }

        return opt;
    }

    RocksDBNativeMetricOptions getMemoryWatcherOptions(
            RocksDBNativeMetricOptions defaultMetricOptions) {
        return optionsFactory == null
                ? defaultMetricOptions
                : optionsFactory.createNativeMetricsOptions(defaultMetricOptions);
    }

    PredefinedOptions getPredefinedOptions() {
        return predefinedOptions;
    }

    @Nullable
    RocksDBOptionsFactory getOptionsFactory() {
        return optionsFactory;
    }

    @Override
    public void close() throws Exception {
        handlesToClose.forEach(IOUtils::closeQuietly);
        handlesToClose.clear();

        if (sharedResources != null) {
            sharedResources.close();
        }
    }

    /**
     * Overwrite configured {@link Filter} if enable partitioned filter. Partitioned filter only
     * worked in full bloom filter, not blocked based.
     */
    private boolean overwriteFilterIfExist(BlockBasedTableConfig blockBasedTableConfig) {
        if (blockBasedTableConfig.filterPolicy() != null) {
            // TODO Can get filter's config in the future RocksDB version, and build new filter use
            // existing config.
            BloomFilter newFilter = new BloomFilter(10, false);
            LOG.info(
                    "Existing filter has been overwritten to full filters since partitioned index filters is enabled.");
            blockBasedTableConfig.setFilterPolicy(newFilter);
            handlesToClose.add(newFilter);
        }
        return true;
    }

    /** Create a {@link DBOptions} for RocksDB, including some common settings. */
    DBOptions createBaseCommonDBOptions() {
        return new DBOptions().setUseFsync(false).setStatsDumpPeriodSec(0);
    }

    /** Create a {@link ColumnFamilyOptions} for RocksDB, including some common settings. */
    ColumnFamilyOptions createBaseCommonColumnOptions() {
        return new ColumnFamilyOptions();
    }

    /**
     * Get a value for option from pre-defined option and configurable option settings. The priority
     * relationship is as below.
     *
     * <p>Configured value > pre-defined value > default value.
     *
     * @param option the wanted option
     * @param <T> the value type
     * @return the final value for the option according to the priority above.
     */
    @Nullable
    private <T> T internalGetOption(ConfigOption<T> option) {
        return configuration
                .getOptional(option)
                .orElseGet(() -> predefinedOptions.getValue(option));
    }

    @SuppressWarnings("ConstantConditions")
    private DBOptions setDBOptionsFromConfigurableOptions(DBOptions currentOptions) {

        currentOptions.setIncreaseParallelism(
                internalGetOption(RocksDBConfigurableOptions.MAX_BACKGROUND_THREADS));

        currentOptions.setMaxOpenFiles(
                internalGetOption(RocksDBConfigurableOptions.MAX_OPEN_FILES));

        currentOptions.setInfoLogLevel(internalGetOption(RocksDBConfigurableOptions.LOG_LEVEL));

        String logDir = internalGetOption(RocksDBConfigurableOptions.LOG_DIR);
        if (logDir != null && !logDir.isEmpty()) {
            currentOptions.setDbLogDir(logDir);
        }

        currentOptions.setMaxLogFileSize(
                internalGetOption(RocksDBConfigurableOptions.LOG_MAX_FILE_SIZE).getBytes());

        currentOptions.setKeepLogFileNum(
                internalGetOption(RocksDBConfigurableOptions.LOG_FILE_NUM));

        return currentOptions;
    }

    @SuppressWarnings("ConstantConditions")
    private ColumnFamilyOptions setColumnFamilyOptionsFromConfigurableOptions(
            ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {

        currentOptions.setCompactionStyle(
                internalGetOption(RocksDBConfigurableOptions.COMPACTION_STYLE));

        currentOptions.setLevelCompactionDynamicLevelBytes(
                internalGetOption(RocksDBConfigurableOptions.USE_DYNAMIC_LEVEL_SIZE));

        currentOptions.setTargetFileSizeBase(
                internalGetOption(RocksDBConfigurableOptions.TARGET_FILE_SIZE_BASE).getBytes());

        currentOptions.setMaxBytesForLevelBase(
                internalGetOption(RocksDBConfigurableOptions.MAX_SIZE_LEVEL_BASE).getBytes());

        currentOptions.setWriteBufferSize(
                internalGetOption(RocksDBConfigurableOptions.WRITE_BUFFER_SIZE).getBytes());

        currentOptions.setMaxWriteBufferNumber(
                internalGetOption(RocksDBConfigurableOptions.MAX_WRITE_BUFFER_NUMBER));

        currentOptions.setMinWriteBufferNumberToMerge(
                internalGetOption(RocksDBConfigurableOptions.MIN_WRITE_BUFFER_NUMBER_TO_MERGE));

        TableFormatConfig tableFormatConfig = currentOptions.tableFormatConfig();

        BlockBasedTableConfig blockBasedTableConfig;
        if (tableFormatConfig == null) {
            blockBasedTableConfig = new BlockBasedTableConfig();
        } else {
            if (tableFormatConfig instanceof PlainTableConfig) {
                // if the table format config is PlainTableConfig, we just return current
                // column-family options
                return currentOptions;
            } else {
                blockBasedTableConfig = (BlockBasedTableConfig) tableFormatConfig;
            }
        }

        blockBasedTableConfig.setBlockSize(
                internalGetOption(RocksDBConfigurableOptions.BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setMetadataBlockSize(
                internalGetOption(RocksDBConfigurableOptions.METADATA_BLOCK_SIZE).getBytes());

        blockBasedTableConfig.setBlockCacheSize(
                internalGetOption(RocksDBConfigurableOptions.BLOCK_CACHE_SIZE).getBytes());

        if (internalGetOption(RocksDBConfigurableOptions.USE_BLOOM_FILTER)) {
            final double bitsPerKey =
                    internalGetOption(RocksDBConfigurableOptions.BLOOM_FILTER_BITS_PER_KEY);
            final boolean blockBasedMode =
                    internalGetOption(RocksDBConfigurableOptions.BLOOM_FILTER_BLOCK_BASED_MODE);
            BloomFilter bloomFilter = new BloomFilter(bitsPerKey, blockBasedMode);
            handlesToClose.add(bloomFilter);
            blockBasedTableConfig.setFilterPolicy(bloomFilter);
        }

        return currentOptions.setTableFormatConfig(blockBasedTableConfig);
    }
}
