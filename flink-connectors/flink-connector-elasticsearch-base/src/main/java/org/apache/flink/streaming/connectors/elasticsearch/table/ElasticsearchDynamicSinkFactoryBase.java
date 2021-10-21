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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.commons.lang3.StringUtils.capitalize;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_DELAY_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_BACKOFF_TYPE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_INTERVAL_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_ACTIONS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.BULK_FLUSH_MAX_SIZE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.CONNECTION_PATH_PREFIX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.DELIVERY_GUARANTEE_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.FORMAT_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.INDEX_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.KEY_DELIMITER_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.PASSWORD_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchConnectorOptions.USERNAME_OPTION;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A {@link DynamicTableSinkFactory} for discovering ElasticsearchDynamicSink. */
@Internal
abstract class ElasticsearchDynamicSinkFactoryBase implements DynamicTableSinkFactory {

    private final String factoryIdentifier;
    private final ElasticsearchSinkBuilderSupplier<RowData> sinkBuilderSupplier;

    public ElasticsearchDynamicSinkFactoryBase(
            String factoryIdentifier,
            ElasticsearchSinkBuilderSupplier<RowData> sinkBuilderSupplier) {
        this.factoryIdentifier = checkNotNull(factoryIdentifier);
        this.sinkBuilderSupplier = checkNotNull(sinkBuilderSupplier);
    }

    @Nullable
    String getDocumentType(Context context) {
        return null; // document type is only set in Elasticsearch versions < 7
    }

    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        List<LogicalTypeWithIndex> primaryKeyLogicalTypesWithIndex =
                getPrimaryKeyLogicalTypesWithIndex(context);
        EncodingFormat<SerializationSchema<RowData>> format =
                getValidatedEncodingFormat(this, context);

        ElasticsearchConfiguration config = getConfiguration(context);
        validateConfiguration(config);

        return new ElasticsearchDynamicSink(
                format,
                config,
                primaryKeyLogicalTypesWithIndex,
                context.getPhysicalRowDataType(),
                capitalize(factoryIdentifier),
                sinkBuilderSupplier,
                getDocumentType(context));
    }

    ElasticsearchConfiguration getConfiguration(Context context) {
        return new ElasticsearchConfiguration(
                Configuration.fromMap(context.getCatalogTable().getOptions()));
    }

    void validateConfiguration(ElasticsearchConfiguration config) {
        config.getHosts(); // validate hosts
        validate(
                config.getIndex().length() >= 1,
                () -> String.format("'%s' must not be empty", INDEX_OPTION.key()));
        int maxActions = config.getBulkFlushMaxActions();
        validate(
                maxActions == -1 || maxActions >= 1,
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_MAX_ACTIONS_OPTION.key(), maxActions));
        long maxSize = config.getBulkFlushMaxByteSize().getBytes();
        long mb1 = 1024 * 1024;
        validate(
                maxSize == -1 || (maxSize >= mb1 && maxSize % mb1 == 0),
                () ->
                        String.format(
                                "'%s' must be in MB granularity. Got: %s",
                                BULK_FLUSH_MAX_SIZE_OPTION.key(),
                                config.getBulkFlushMaxByteSize().toHumanReadableString()));
        validate(
                config.getBulkFlushBackoffRetries().map(retries -> retries >= 1).orElse(true),
                () ->
                        String.format(
                                "'%s' must be at least 1. Got: %s",
                                BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION.key(),
                                config.getBulkFlushBackoffRetries().get()));
        if (config.getUsername().isPresent()
                && !StringUtils.isNullOrWhitespaceOnly(config.getUsername().get())) {
            validate(
                    config.getPassword().isPresent()
                            && !StringUtils.isNullOrWhitespaceOnly(config.getPassword().get()),
                    () ->
                            String.format(
                                    "'%s' and '%s' must be set at the same time. Got: username '%s' and password '%s'",
                                    USERNAME_OPTION.key(),
                                    PASSWORD_OPTION.key(),
                                    config.getUsername().get(),
                                    config.getPassword().orElse("")));
        }
    }

    static void validate(boolean condition, Supplier<String> message) {
        if (!condition) {
            throw new ValidationException(message.get());
        }
    }

    EncodingFormat<SerializationSchema<RowData>> getValidatedEncodingFormat(
            DynamicTableFactory factory, DynamicTableFactory.Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(factory, context);
        final EncodingFormat<SerializationSchema<RowData>> format =
                helper.discoverEncodingFormat(SerializationFormatFactory.class, FORMAT_OPTION);
        helper.validate();
        return format;
    }

    List<LogicalTypeWithIndex> getPrimaryKeyLogicalTypesWithIndex(Context context) {
        DataType physicalRowDataType = context.getPhysicalRowDataType();
        int[] primaryKeyIndexes = context.getPrimaryKeyIndexes();
        if (primaryKeyIndexes.length != 0) {
            DataType pkDataType = DataType.projectFields(physicalRowDataType, primaryKeyIndexes);

            ElasticsearchValidationUtils.validatePrimaryKey(pkDataType);
        }

        ResolvedSchema resolvedSchema = context.getCatalogTable().getResolvedSchema();
        return Arrays.stream(primaryKeyIndexes)
                .mapToObj(
                        index -> {
                            Optional<Column> column = resolvedSchema.getColumn(index);
                            if (!column.isPresent()) {
                                throw new IllegalStateException(
                                        String.format(
                                                "No primary key column found with index '%s'.",
                                                index));
                            }
                            LogicalType logicalType = column.get().getDataType().getLogicalType();
                            return new LogicalTypeWithIndex(index, logicalType);
                        })
                .collect(Collectors.toList());
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Stream.of(HOSTS_OPTION, INDEX_OPTION).collect(Collectors.toSet());
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Stream.of(
                        KEY_DELIMITER_OPTION,
                        BULK_FLUSH_MAX_SIZE_OPTION,
                        BULK_FLUSH_MAX_ACTIONS_OPTION,
                        BULK_FLUSH_INTERVAL_OPTION,
                        BULK_FLUSH_BACKOFF_TYPE_OPTION,
                        BULK_FLUSH_BACKOFF_MAX_RETRIES_OPTION,
                        BULK_FLUSH_BACKOFF_DELAY_OPTION,
                        CONNECTION_PATH_PREFIX_OPTION,
                        FORMAT_OPTION,
                        DELIVERY_GUARANTEE_OPTION,
                        PASSWORD_OPTION,
                        USERNAME_OPTION)
                .collect(Collectors.toSet());
    }

    @Override
    public String factoryIdentifier() {
        return factoryIdentifier;
    }
}
