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

package org.apache.flink.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.CommonCatalogOptions;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSinkMock;
import org.apache.flink.table.factories.TestDynamicTableFactory.DynamicTableSourceMock;
import org.apache.flink.table.factories.TestFormatFactory.DecodingFormatMock;
import org.apache.flink.table.factories.TestFormatFactory.EncodingFormatMock;
import org.apache.flink.table.factories.utils.FactoryMocks;
import org.apache.flink.testutils.ClassLoaderUtils;

import org.assertj.core.api.AbstractThrowableAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.table.factories.utils.FactoryMocks.SCHEMA;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSink;
import static org.apache.flink.table.factories.utils.FactoryMocks.createTableSource;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link FactoryUtil}. */
public class FactoryUtilTest {

    @Test
    public void testManagedConnector() {
        final Map<String, String> options = createAllOptions();
        options.remove("connector");
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        assertThat(actualSource)
                .isExactlyInstanceOf(TestManagedTableFactory.TestManagedTableSource.class);
    }

    @Test
    public void testInvalidConnector() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.put("connector", "FAIL"),
                "Could not find any factory for identifier 'FAIL' that implements '"
                        + DynamicTableFactory.class.getName()
                        + "' in the classpath.\n\n"
                        + "Available factory identifiers are:\n\n"
                        + "conflicting\nsink-only\nsource-only\ntest\ntest-connector");
    }

    @Test
    public void testConflictingConnector() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.put("connector", TestConflictingDynamicTableFactory1.IDENTIFIER),
                "Multiple factories for identifier 'conflicting' that implement '"
                        + DynamicTableFactory.class.getName()
                        + "' found in the classpath.\n"
                        + "\n"
                        + "Ambiguous factory classes are:\n"
                        + "\n"
                        + TestConflictingDynamicTableFactory1.class.getName()
                        + "\n"
                        + TestConflictingDynamicTableFactory2.class.getName());
    }

    @Test
    public void testMissingConnectorOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.remove("target"),
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "target");
    }

    @Test
    public void testInvalidConnectorOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.put("buffer-size", "FAIL"),
                "Invalid value for option 'buffer-size'.");
    }

    @Test
    public void testMissingFormat() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.remove("value.format"),
                "Could not find required scan format 'value.format'.");
    }

    @Test
    public void testInvalidFormat() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.put("value.format", "FAIL"),
                "Could not find any factory for identifier 'FAIL' that implements '"
                        + DeserializationFormatFactory.class.getName()
                        + "' in the classpath.\n\n"
                        + "Available factory identifiers are:\n\n"
                        + "test-format");
    }

    @Test
    public void testMissingFormatOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.remove("key.test-format.delimiter"),
                "One or more required options are missing.\n\n"
                        + "Missing required options are:\n\n"
                        + "delimiter",
                "Error creating scan format 'test-format' in option space 'key.test-format.'.");
    }

    @Test
    public void testInvalidFormatOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> options.put("key.test-format.fail-on-missing", "FAIL"),
                "Invalid value for option 'fail-on-missing'.");
    }

    @Test
    public void testSecretOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> {
                    options.remove("target");
                    options.put("password", "123");
                },
                "Table options are:\n"
                        + "\n"
                        + "'buffer-size'='1000'\n"
                        + "'connector'='test-connector'\n"
                        + "'key.format'='test-format'\n"
                        + "'key.test-format.delimiter'=','\n"
                        + "'password'='******'\n"
                        + "'property-version'='1'\n"
                        + "'value.format'='test-format'\n"
                        + "'value.test-format.delimiter'='|'\n"
                        + "'value.test-format.fail-on-missing'='true'");
    }

    @Test
    public void testUnconsumedOption() {
        assertCreateTableSourceWithOptionModifier(
                options -> {
                    options.put("this-is-not-consumed", "42");
                    options.put("this-is-also-not-consumed", "true");
                },
                "Unsupported options found for 'test-connector'.\n\n"
                        + "Unsupported options:\n\n"
                        + "this-is-also-not-consumed\n"
                        + "this-is-not-consumed\n\n"
                        + "Supported options:\n\n"
                        + "buffer-size\n"
                        + "connector\n"
                        + "deprecated-target (deprecated)\n"
                        + "fallback-buffer-size\n"
                        + "format\n"
                        + "key.format\n"
                        + "key.test-format.changelog-mode\n"
                        + "key.test-format.delimiter\n"
                        + "key.test-format.deprecated-delimiter (deprecated)\n"
                        + "key.test-format.fail-on-missing\n"
                        + "key.test-format.fallback-fail-on-missing\n"
                        + "key.test-format.readable-metadata\n"
                        + "password\n"
                        + "property-version\n"
                        + "target\n"
                        + "value.format\n"
                        + "value.test-format.changelog-mode\n"
                        + "value.test-format.delimiter\n"
                        + "value.test-format.deprecated-delimiter (deprecated)\n"
                        + "value.test-format.fail-on-missing\n"
                        + "value.test-format.fallback-fail-on-missing\n"
                        + "value.test-format.readable-metadata");
    }

    @Test
    public void testAllOptions() {
        final Map<String, String> options = createAllOptions();
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock("|", true));
        assertThat(actualSource).isEqualTo(expectedSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock("|"));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testDiscoveryForSeparateSourceSinkFactory() {
        final Map<String, String> options = createAllOptions();
        // the "test" source and sink factory is not in one factory class
        // see TestDynamicTableSinkFactory and TestDynamicTableSourceFactory
        options.put("connector", "test");

        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock("|", true));
        assertThat(actualSource).isEqualTo(expectedSource);

        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock("|"));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testOptionalFormat() {
        final Map<String, String> options = createAllOptions();
        options.remove("key.format");
        options.remove("key.test-format.delimiter");
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock("MyTarget", null, new DecodingFormatMock("|", true));
        assertThat(actualSource).isEqualTo(expectedSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock("MyTarget", 1000L, null, new EncodingFormatMock("|"));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testAlternativeValueFormat() {
        final Map<String, String> options = createAllOptions();
        options.remove("value.format");
        options.remove("value.test-format.delimiter");
        options.remove("value.test-format.fail-on-missing");
        options.put("format", "test-format");
        options.put("test-format.delimiter", ";");
        options.put("test-format.fail-on-missing", "true");
        final DynamicTableSource actualSource = createTableSource(SCHEMA, options);
        final DynamicTableSource expectedSource =
                new DynamicTableSourceMock(
                        "MyTarget",
                        new DecodingFormatMock(",", false),
                        new DecodingFormatMock(";", true));
        assertThat(actualSource).isEqualTo(expectedSource);
        final DynamicTableSink actualSink = createTableSink(SCHEMA, options);
        final DynamicTableSink expectedSink =
                new DynamicTableSinkMock(
                        "MyTarget",
                        1000L,
                        new EncodingFormatMock(","),
                        new EncodingFormatMock(";"));
        assertThat(actualSink).isEqualTo(expectedSink);
    }

    @Test
    public void testConnectorErrorHint() {
        assertThatThrownBy(
                        () ->
                                createTableSource(
                                        SCHEMA, Collections.singletonMap("connector", "sink-only")))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Connector 'sink-only' can only be used as a sink. It cannot be used as a source."));

        assertThatThrownBy(
                        () ->
                                createTableSink(
                                        SCHEMA,
                                        Collections.singletonMap("connector", "source-only")))
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Connector 'source-only' can only be used as a source. It cannot be used as a sink."));
    }

    @Test
    public void testRequiredPlaceholderOption() {
        final Set<ConfigOption<?>> requiredOptions = new HashSet<>();
        requiredOptions.add(ConfigOptions.key("fields.#.min").intType().noDefaultValue());
        requiredOptions.add(
                ConfigOptions.key("no.placeholder.anymore")
                        .intType()
                        .noDefaultValue()
                        .withFallbackKeys("old.fields.#.min"));

        FactoryUtil.validateFactoryOptions(requiredOptions, new HashSet<>(), new Configuration());
    }

    @Test
    public void testCreateCatalog() {
        final Map<String, String> options = new HashMap<>();
        options.put(CommonCatalogOptions.CATALOG_TYPE.key(), TestCatalogFactory.IDENTIFIER);
        options.put(TestCatalogFactory.DEFAULT_DATABASE.key(), "my-database");

        final Catalog catalog =
                FactoryUtil.createCatalog(
                        "my-catalog",
                        options,
                        null,
                        Thread.currentThread().getContextClassLoader());
        assertThat(catalog).isInstanceOf(TestCatalogFactory.TestCatalog.class);

        final TestCatalogFactory.TestCatalog testCatalog = (TestCatalogFactory.TestCatalog) catalog;
        assertThat("my-catalog").isEqualTo(testCatalog.getName());
        assertThat("my-database")
                .isEqualTo(testCatalog.getOptions().get(TestCatalogFactory.DEFAULT_DATABASE.key()));
    }

    @Test
    public void testCatalogFactoryHelper() {
        final FactoryUtil.CatalogFactoryHelper helper1 =
                FactoryUtil.createCatalogFactoryHelper(
                        new TestCatalogFactory(),
                        new FactoryUtil.DefaultCatalogContext(
                                "test",
                                Collections.emptyMap(),
                                null,
                                Thread.currentThread().getContextClassLoader()));

        // No error
        helper1.validate();

        final FactoryUtil.CatalogFactoryHelper helper2 =
                FactoryUtil.createCatalogFactoryHelper(
                        new TestCatalogFactory(),
                        new FactoryUtil.DefaultCatalogContext(
                                "test",
                                Collections.singletonMap("x", "y"),
                                null,
                                Thread.currentThread().getContextClassLoader()));

        assertThatThrownBy(helper2::validate)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unsupported options found for 'test-catalog'"));
    }

    @Test
    public void testFactoryHelperWithDeprecatedOptions() {
        final Map<String, String> options = new HashMap<>();
        options.put("deprecated-target", "MyTarget");
        options.put("fallback-buffer-size", "1000");
        options.put("value.format", "test-format");
        options.put("value.test-format.deprecated-delimiter", "|");
        options.put("value.test-format.fallback-fail-on-missing", "true");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestDynamicTableFactory(),
                        FactoryMocks.createTableContext(SCHEMA, options));
        helper.discoverDecodingFormat(
                DeserializationFormatFactory.class, TestDynamicTableFactory.VALUE_FORMAT);
        helper.validate();
    }

    @Test
    public void testFactoryHelperWithMapOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("properties.prop-1", "value-1");
        options.put("properties.prop-2", "value-2");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestFactoryWithMap(), FactoryMocks.createTableContext(SCHEMA, options));

        helper.validate();
    }

    @Test
    public void testInvalidFactoryHelperWithMapOption() {
        final Map<String, String> options = new HashMap<>();
        options.put("properties.prop-1", "value-1");
        options.put("properties.prop-2", "value-2");
        options.put("unknown", "value-3");

        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(
                        new TestFactoryWithMap(), FactoryMocks.createTableContext(SCHEMA, options));

        assertThatThrownBy(helper::validate)
                .satisfies(
                        anyCauseMatches(
                                ValidationException.class,
                                "Unsupported options found for 'test-factory-with-map'.\n\n"
                                        + "Unsupported options:\n\n"
                                        + "unknown\n\n"
                                        + "Supported options:\n\n"
                                        + "connector\n"
                                        + "properties\n"
                                        + "properties.prop-1\n"
                                        + "properties.prop-2\n"
                                        + "property-version"));
    }

    @Test
    public void testDiscoverFactoryBadClass(@TempDir Path tempDir) throws IOException {
        // Let's prepare the classloader with a factory interface and 2 classes, one implements our
        // sub-interface of SerializationFormatFactory and the other implements only
        // SerializationFormatFactory.
        final String subInterfaceName = "MyFancySerializationSchemaFormat";
        final String subInterfaceImplementationName = "MyFancySerializationSchemaFormatImpl";
        final String serializationSchemaImplementationName = "AnotherSerializationSchema";

        final URLClassLoader classLoaderIncludingTheInterface =
                ClassLoaderUtils.withRoot(tempDir.toFile())
                        .addClass(
                                subInterfaceName,
                                "public interface "
                                        + subInterfaceName
                                        + " extends "
                                        + SerializationFormatFactory.class.getName()
                                        + " {}")
                        .addClass(
                                subInterfaceImplementationName,
                                "import org.apache.flink.api.common.serialization.SerializationSchema;"
                                        + "import org.apache.flink.configuration.ConfigOption;"
                                        + "import org.apache.flink.configuration.ReadableConfig;"
                                        + "import org.apache.flink.table.connector.format.EncodingFormat;"
                                        + "import org.apache.flink.table.data.RowData;"
                                        + "import org.apache.flink.table.factories.DynamicTableFactory;"
                                        + "import org.apache.flink.table.factories.SerializationFormatFactory;"
                                        + "import java.util.Set;"
                                        + "public class "
                                        + subInterfaceImplementationName
                                        + " implements "
                                        + subInterfaceName
                                        + " {"
                                        + "@Override public String factoryIdentifier() { return null; }"
                                        + "@Override public Set<ConfigOption<?>> requiredOptions() { return null; }"
                                        + "@Override public Set<ConfigOption<?>> optionalOptions() { return null; }"
                                        + "@Override public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) { return null; }"
                                        + "}")
                        .addClass(
                                serializationSchemaImplementationName,
                                "import org.apache.flink.api.common.serialization.SerializationSchema;"
                                        + "import org.apache.flink.configuration.ConfigOption;"
                                        + "import org.apache.flink.configuration.ReadableConfig;"
                                        + "import org.apache.flink.table.connector.format.EncodingFormat;"
                                        + "import org.apache.flink.table.data.RowData;"
                                        + "import org.apache.flink.table.factories.DynamicTableFactory;"
                                        + "import org.apache.flink.table.factories.SerializationFormatFactory;"
                                        + "import java.util.Set;"
                                        + "public class "
                                        + serializationSchemaImplementationName
                                        + " implements "
                                        + SerializationFormatFactory.class.getName()
                                        + " {"
                                        + "@Override public String factoryIdentifier() { return null; }"
                                        + "@Override public Set<ConfigOption<?>> requiredOptions() { return null; }"
                                        + "@Override public Set<ConfigOption<?>> optionalOptions() { return null; }"
                                        + "@Override public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(DynamicTableFactory.Context context, ReadableConfig formatOptions) { return null; }"
                                        + "}")
                        .addService(Factory.class.getName(), subInterfaceImplementationName)
                        .addService(Factory.class.getName(), serializationSchemaImplementationName)
                        .build();

        // Delete the sub interface now, so it can't be loaded
        Files.delete(tempDir.resolve(subInterfaceName + ".class"));

        assertThat(FactoryUtil.discoverFactories(classLoaderIncludingTheInterface))
                .map(f -> f.getClass().getName())
                .doesNotContain(subInterfaceImplementationName)
                .contains(serializationSchemaImplementationName);
    }

    // --------------------------------------------------------------------------------------------
    // Helper methods
    // --------------------------------------------------------------------------------------------

    private static void assertCreateTableSourceWithOptionModifier(
            Consumer<Map<String, String>> optionModifier, String... messages) {
        AbstractThrowableAssert<?, ? extends Throwable> assertion =
                assertThatThrownBy(
                        () -> {
                            final Map<String, String> options = createAllOptions();
                            optionModifier.accept(options);
                            createTableSource(SCHEMA, options);
                        });

        for (String message : messages) {
            assertion.satisfies(anyCauseMatches(ValidationException.class, message));
        }
    }

    private static Map<String, String> createAllOptions() {
        final Map<String, String> options = new HashMap<>();
        // we use strings here to test realistic parsing
        options.put("property-version", "1");
        options.put("connector", TestDynamicTableFactory.IDENTIFIER);
        options.put("target", "MyTarget");
        options.put("buffer-size", "1000");
        options.put("key.format", "test-format");
        options.put("key.test-format.delimiter", ",");
        options.put("value.format", "test-format");
        options.put("value.test-format.delimiter", "|");
        options.put("value.test-format.fail-on-missing", "true");
        return options;
    }

    // --------------------------------------------------------------------------------------------
    // Helper classes
    // --------------------------------------------------------------------------------------------

    private static class TestFactoryWithMap implements DynamicTableFactory {

        public static final ConfigOption<Map<String, String>> PROPERTIES =
                ConfigOptions.key("properties").mapType().noDefaultValue();

        @Override
        public String factoryIdentifier() {
            return "test-factory-with-map";
        }

        @Override
        public Set<ConfigOption<?>> requiredOptions() {
            return Collections.emptySet();
        }

        @Override
        public Set<ConfigOption<?>> optionalOptions() {
            return Collections.singleton(PROPERTIES);
        }
    }
}
