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

package org.apache.flink.formats.json.canal;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.json.JsonOptions;
import org.apache.flink.formats.json.TimestampFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Tests for {@link CanalJsonFormatFactory}. */
public class CanalJsonFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    private static final TableSchema SCHEMA =
            TableSchema.builder()
                    .field("a", DataTypes.STRING())
                    .field("b", DataTypes.INT())
                    .field("c", DataTypes.BOOLEAN())
                    .build();

    private static final DataType PHYSICAL_DATA_TYPE = SCHEMA.toPhysicalRowDataType();

    private static final RowType ROW_TYPE = (RowType) PHYSICAL_DATA_TYPE.getLogicalType();

    private static final InternalTypeInfo<RowData> ROW_TYPE_INFO = InternalTypeInfo.of(ROW_TYPE);

    @Test
    public void testDefaultOptions() {
        Map<String, String> options = getAllOptions();

        // test Deser
        CanalJsonDeserializationSchema expectedDeser =
                CanalJsonDeserializationSchema.builder(
                                PHYSICAL_DATA_TYPE, Collections.emptyList(), ROW_TYPE_INFO)
                        .setIgnoreParseErrors(false)
                        .setTimestampFormat(TimestampFormat.SQL)
                        .build();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        // test Ser
        CanalJsonSerializationSchema expectedSer =
                new CanalJsonSerializationSchema(
                        ROW_TYPE, TimestampFormat.SQL, JsonOptions.MapNullKeyMode.FAIL, "null");
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testUserDefinedOptions() {
        Map<String, String> options = getAllOptions();
        options.put("canal-json.ignore-parse-errors", "true");
        options.put("canal-json.timestamp-format.standard", "ISO-8601");
        options.put("canal-json.database.include", "mydb");
        options.put("canal-json.table.include", "mytable");
        options.put("canal-json.map-null-key.mode", "LITERAL");
        options.put("canal-json.map-null-key.literal", "nullKey");

        // test Deser
        CanalJsonDeserializationSchema expectedDeser =
                CanalJsonDeserializationSchema.builder(
                                PHYSICAL_DATA_TYPE, Collections.emptyList(), ROW_TYPE_INFO)
                        .setIgnoreParseErrors(true)
                        .setTimestampFormat(TimestampFormat.ISO_8601)
                        .setDatabase("mydb")
                        .setTable("mytable")
                        .build();
        DeserializationSchema<RowData> actualDeser = createDeserializationSchema(options);
        assertEquals(expectedDeser, actualDeser);

        // test Ser
        CanalJsonSerializationSchema expectedSer =
                new CanalJsonSerializationSchema(
                        ROW_TYPE,
                        TimestampFormat.ISO_8601,
                        JsonOptions.MapNullKeyMode.LITERAL,
                        "nullKey");
        SerializationSchema<RowData> actualSer = createSerializationSchema(options);
        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testInvalidIgnoreParseError() {
        thrown.expect(
                containsCause(
                        new IllegalArgumentException(
                                "Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("canal-json.ignore-parse-errors", "abc"));

        createDeserializationSchema(options);
    }

    @Test
    public void testInvalidOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifiedOptions(
                        opts -> opts.put("canal-json.timestamp-format.standard", "test"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'test' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
        createDeserializationSchema(tableOptions);
    }

    @Test
    public void testInvalidOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifiedOptions(opts -> opts.put("canal-json.map-null-key.mode", "invalid"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'invalid' for option map-null-key.mode. Supported values are [LITERAL, FAIL, DROP].")));
        createSerializationSchema(tableOptions);
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    /**
     * Returns the full options modified by the given consumer {@code optionModifier}.
     *
     * @param optionModifier Consumer to modify the options
     */
    private Map<String, String> getModifiedOptions(Consumer<Map<String, String>> optionModifier) {
        Map<String, String> options = getAllOptions();
        optionModifier.accept(options);
        return options;
    }

    private Map<String, String> getAllOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");
        options.put("format", "canal-json");
        return options;
    }

    private static DeserializationSchema<RowData> createDeserializationSchema(
            Map<String, String> options) {
        DynamicTableSource source =
                FactoryUtil.createTableSource(
                        null,
                        ObjectIdentifier.of("default", "default", "t1"),
                        new CatalogTableImpl(SCHEMA, options, "mock source"),
                        new Configuration(),
                        CanalJsonFormatFactoryTest.class.getClassLoader(),
                        false);

        assert source instanceof TestDynamicTableFactory.DynamicTableSourceMock;
        TestDynamicTableFactory.DynamicTableSourceMock scanSourceMock =
                (TestDynamicTableFactory.DynamicTableSourceMock) source;

        return scanSourceMock.valueFormat.createRuntimeDecoder(
                ScanRuntimeProviderContext.INSTANCE, PHYSICAL_DATA_TYPE);
    }

    private static SerializationSchema<RowData> createSerializationSchema(
            Map<String, String> options) {
        DynamicTableSink sink =
                FactoryUtil.createTableSink(
                        null,
                        ObjectIdentifier.of("default", "default", "t1"),
                        new CatalogTableImpl(SCHEMA, options, "mock sink"),
                        new Configuration(),
                        CanalJsonFormatFactoryTest.class.getClassLoader(),
                        false);

        assert sink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) sink;

        return sinkMock.valueFormat.createRuntimeEncoder(
                new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE);
    }
}
