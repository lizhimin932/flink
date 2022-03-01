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

package org.apache.flink.formats.json.ogg;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonFormatOptions;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.TestDynamicTableFactory;
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.table.factories.utils.FactoryMocks.PHYSICAL_DATA_TYPE;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.junit.Assert.assertEquals;

/** Tests for {@link OggJsonFormatFactory}. */
public class OggJsonFormatFactoryTest extends TestLogger {
    @Rule public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testSeDeSchema() {
        final Map<String, String> options = getAllOptions();

        final OggJsonSerializationSchema expectedSer =
                new OggJsonSerializationSchema(
                        (RowType) PHYSICAL_DATA_TYPE.getLogicalType(),
                        TimestampFormat.ISO_8601,
                        JsonFormatOptions.MapNullKeyMode.LITERAL,
                        "null",
                        true);

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        assert actualSink instanceof TestDynamicTableFactory.DynamicTableSinkMock;
        TestDynamicTableFactory.DynamicTableSinkMock sinkMock =
                (TestDynamicTableFactory.DynamicTableSinkMock) actualSink;

        SerializationSchema<RowData> actualSer =
                sinkMock.valueFormat.createRuntimeEncoder(
                        new SinkRuntimeProviderContext(false), PHYSICAL_DATA_TYPE);

        assertEquals(expectedSer, actualSer);
    }

    @Test
    public void testInvalidIgnoreParseError() {
        thrown.expect(
                containsCause(
                        new IllegalArgumentException(
                                "Unrecognized option for boolean: abc. Expected either true or false(case insensitive)")));

        final Map<String, String> options =
                getModifiedOptions(opts -> opts.put("ogg-json.ignore-parse-errors", "abc"));

        createTableSource(SCHEMA, options);
    }

    @Test
    public void testInvalidOptionForTimestampFormat() {
        final Map<String, String> tableOptions =
                getModifiedOptions(opts -> opts.put("ogg-json.timestamp-format.standard", "test"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'test' for timestamp-format.standard. Supported values are [SQL, ISO-8601].")));
        createTableSource(SCHEMA, tableOptions);
    }

    @Test
    public void testInvalidOptionForMapNullKeyMode() {
        final Map<String, String> tableOptions =
                getModifiedOptions(opts -> opts.put("ogg-json.map-null-key.mode", "invalid"));

        thrown.expect(ValidationException.class);
        thrown.expect(
                containsCause(
                        new ValidationException(
                                "Unsupported value 'invalid' for option map-null-key.mode. Supported values are [LITERAL, FAIL, DROP].")));
        createTableSink(SCHEMA, tableOptions);
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

        options.put("format", "ogg-json");
        options.put("ogg-json.ignore-parse-errors", "true");
        options.put("ogg-json.timestamp-format.standard", "ISO-8601");
        options.put("ogg-json.map-null-key.mode", "LITERAL");
        options.put("ogg-json.map-null-key.literal", "null");
        options.put("ogg-json.encode.decimal-as-plain-number", "true");
        return options;
    }
}
