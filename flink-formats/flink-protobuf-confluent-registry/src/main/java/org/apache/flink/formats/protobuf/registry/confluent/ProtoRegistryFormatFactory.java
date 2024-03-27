/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.protobuf.registry.confluent;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.protobuf.registry.confluent.utils.FlinkToProtoSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory.Context;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashSet;
import java.util.Set;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema;

/**
 * Table format factory for providing configured instances of Schema Registry Protobuf to RowData
 * {@link SerializationSchema} and {@link DeserializationSchema}.
 */
public class ProtoRegistryFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "proto-confluent";
    private static final String ROW = "row";
    private static final String PACKAGE = "io.confluent.generated";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            Context context, ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                ProtobufSchema rowSchema = FlinkToProtoSchemaConverter.fromFlinkRowType(rowType,ROW,PACKAGE);
                final SchemaCoder schemaCoder = SchemaRegistryClientFactory.getCoder(rowSchema,
                        formatOptions);
                return new ProtoRegistryDeserializationSchema(schemaCoder,rowType,
                        context.createTypeInformation(physicalDataType));
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            Context context, ReadableConfig formatOptions) {
        final SchemaRegistryClient registryConfig = SchemaRegistryClientFactory.getClient(formatOptions);
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context, DataType physicalDataType) {
                final RowType rowType = (RowType) physicalDataType.getLogicalType();
                ProtobufSchema rowSchema = FlinkToProtoSchemaConverter.fromFlinkRowType(rowType,ROW,PACKAGE);

                final SchemaCoder schemaCoder = SchemaRegistryClientFactory.getCoder(
                        rowSchema,
                        formatOptions);
                return new ProtoRegistrySerializationSchema(schemaCoder, rowSchema, rowType);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return SchemaRegistryClientFactory.getRequiredOptions();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return new HashSet<>(SchemaRegistryClientFactory.getOptionalOptions());
    }

    @Override
    public Set<ConfigOption<?>> forwardOptions() {
        return new HashSet<>(SchemaRegistryClientFactory.getForwardOptions());
    }
}
