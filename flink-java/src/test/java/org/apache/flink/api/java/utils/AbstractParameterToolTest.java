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

package org.apache.flink.api.java.utils;

import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.InstantiationUtil;
import org.junit.Rule;
import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

/** Base class for tests for {@link ParameterTool}. */
public abstract class AbstractParameterToolTest {

    @Rule public TemporaryFolder tmp = new TemporaryFolder();

    @Rule public final ExpectedException exception = ExpectedException.none();

    // Test parser

    @Test
    public void testThrowExceptionIfParameterIsNotPrefixed() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage(
                "Error parsing arguments '[a]' on 'a'. Please prefix keys with -- or -.");

        createParameterToolFromArgs(new String[] {"a"});
    }

    @Test
    public void testNoVal() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-berlin"});
        Assertions.assertEquals(1, parameter.getNumberOfParameters());
        Assertions.assertTrue(parameter.has("berlin"));
    }

    @Test
    public void testNoValDouble() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"--berlin"});
        Assertions.assertEquals(1, parameter.getNumberOfParameters());
        Assertions.assertTrue(parameter.has("berlin"));
    }

    @Test
    public void testMultipleNoVal() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {"--a", "--b", "--c", "--d", "--e", "--f"});
        Assertions.assertEquals(6, parameter.getNumberOfParameters());
        Assertions.assertTrue(parameter.has("a"));
        Assertions.assertTrue(parameter.has("b"));
        Assertions.assertTrue(parameter.has("c"));
        Assertions.assertTrue(parameter.has("d"));
        Assertions.assertTrue(parameter.has("e"));
        Assertions.assertTrue(parameter.has("f"));
    }

    @Test
    public void testMultipleNoValMixed() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"--a", "-b", "-c", "-d", "--e", "--f"});
        Assertions.assertEquals(6, parameter.getNumberOfParameters());
        Assertions.assertTrue(parameter.has("a"));
        Assertions.assertTrue(parameter.has("b"));
        Assertions.assertTrue(parameter.has("c"));
        Assertions.assertTrue(parameter.has("d"));
        Assertions.assertTrue(parameter.has("e"));
        Assertions.assertTrue(parameter.has("f"));
    }

    @Test
    public void testEmptyVal() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The input [--a, -b, --] contains an empty argument");

        createParameterToolFromArgs(new String[] {"--a", "-b", "--"});
    }

    @Test
    public void testEmptyValShort() {
        exception.expect(IllegalArgumentException.class);
        exception.expectMessage("The input [--a, -b, -] contains an empty argument");

        createParameterToolFromArgs(new String[] {"--a", "-b", "-"});
    }

    // Test unrequested
    // Boolean

    @Test
    public void testUnrequestedBoolean() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        Assertions.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertTrue(parameter.getBoolean("boolean"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertTrue(parameter.getBoolean("boolean"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedBooleanWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-boolean", "true"});
        Assertions.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertTrue(parameter.getBoolean("boolean", false));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertTrue(parameter.getBoolean("boolean", false));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedBooleanWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        Assertions.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

        parameter.getBoolean("boolean");
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    // Byte

    @Test
    public void testUnrequestedByte() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        Assertions.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(1, parameter.getByte("byte"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(1, parameter.getByte("byte"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedByteWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte", "1"});
        Assertions.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(1, parameter.getByte("byte", (byte) 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(1, parameter.getByte("byte", (byte) 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedByteWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-byte"});
        Assertions.assertEquals(createHashSet("byte"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getByte("byte");
    }

    // Short

    @Test
    public void testUnrequestedShort() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        Assertions.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(2, parameter.getShort("short"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(2, parameter.getShort("short"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedShortWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short", "2"});
        Assertions.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(2, parameter.getShort("short", (short) 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(2, parameter.getShort("short", (short) 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedShortWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-short"});
        Assertions.assertEquals(createHashSet("short"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getShort("short");
    }

    // Int

    @Test
    public void testUnrequestedInt() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        Assertions.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(4, parameter.getInt("int"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(4, parameter.getInt("int"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedIntWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int", "4"});
        Assertions.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(4, parameter.getInt("int", 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(4, parameter.getInt("int", 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedIntWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-int"});
        Assertions.assertEquals(createHashSet("int"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getInt("int");
    }

    // Long

    @Test
    public void testUnrequestedLong() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        Assertions.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(8, parameter.getLong("long"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(8, parameter.getLong("long"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedLongWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long", "8"});
        Assertions.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(8, parameter.getLong("long", 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(8, parameter.getLong("long", 0));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedLongWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-long"});
        Assertions.assertEquals(createHashSet("long"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getLong("long");
    }

    // Float

    @Test
    public void testUnrequestedFloat() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        Assertions.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedFloatWithDefaultValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float", "4"});
        Assertions.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(4.0, parameter.getFloat("float", 0.0f), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(4.0, parameter.getFloat("float", 0.0f), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedFloatWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-float"});
        Assertions.assertEquals(createHashSet("float"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getFloat("float");
    }

    // Double

    @Test
    public void testUnrequestedDouble() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        Assertions.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedDoubleWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-double", "8"});
        Assertions.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals(8.0, parameter.getDouble("double", 0.0), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals(8.0, parameter.getDouble("double", 0.0), 0.00001);
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedDoubleWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-double"});
        Assertions.assertEquals(createHashSet("double"), parameter.getUnrequestedParameters());

        exception.expect(RuntimeException.class);
        exception.expectMessage("For input string: \"__NO_VALUE_KEY\"");

        parameter.getDouble("double");
    }

    // String

    @Test
    public void testUnrequestedString() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        Assertions.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals("∞", parameter.get("string"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals("∞", parameter.get("string"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedStringWithDefaultValue() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-string", "∞"});
        Assertions.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals("∞", parameter.get("string", "0.0"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals("∞", parameter.get("string", "0.0"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedStringWithMissingValue() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-string"});
        Assertions.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

        parameter.get("string");
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    // Additional methods

    @Test
    public void testUnrequestedHas() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {"-boolean"});
        Assertions.assertEquals(createHashSet("boolean"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertTrue(parameter.has("boolean"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertTrue(parameter.has("boolean"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedRequired() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(new String[] {"-required", "∞"});
        Assertions.assertEquals(createHashSet("required"), parameter.getUnrequestedParameters());

        // test parameter access
        Assertions.assertEquals("∞", parameter.getRequired("required"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        // test repeated access
        Assertions.assertEquals("∞", parameter.getRequired("required"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedMultiple() {
        AbstractParameterTool parameter =
                createParameterToolFromArgs(
                        new String[] {
                            "-boolean",
                            "true",
                            "-byte",
                            "1",
                            "-short",
                            "2",
                            "-int",
                            "4",
                            "-long",
                            "8",
                            "-float",
                            "4.0",
                            "-double",
                            "8.0",
                            "-string",
                            "∞"
                        });
        Assertions.assertEquals(
                createHashSet(
                        "boolean", "byte", "short", "int", "long", "float", "double", "string"),
                parameter.getUnrequestedParameters());

        Assertions.assertTrue(parameter.getBoolean("boolean"));
        Assertions.assertEquals(
                createHashSet("byte", "short", "int", "long", "float", "double", "string"),
                parameter.getUnrequestedParameters());

        Assertions.assertEquals(1, parameter.getByte("byte"));
        Assertions.assertEquals(
                createHashSet("short", "int", "long", "float", "double", "string"),
                parameter.getUnrequestedParameters());

        Assertions.assertEquals(2, parameter.getShort("short"));
        Assertions.assertEquals(
                createHashSet("int", "long", "float", "double", "string"),
                parameter.getUnrequestedParameters());

        Assertions.assertEquals(4, parameter.getInt("int"));
        Assertions.assertEquals(
                createHashSet("long", "float", "double", "string"),
                parameter.getUnrequestedParameters());

        Assertions.assertEquals(8, parameter.getLong("long"));
        Assertions.assertEquals(
                createHashSet("float", "double", "string"), parameter.getUnrequestedParameters());

        Assertions.assertEquals(4.0, parameter.getFloat("float"), 0.00001);
        Assertions.assertEquals(
                createHashSet("double", "string"), parameter.getUnrequestedParameters());

        Assertions.assertEquals(8.0, parameter.getDouble("double"), 0.00001);
        Assertions.assertEquals(createHashSet("string"), parameter.getUnrequestedParameters());

        Assertions.assertEquals("∞", parameter.get("string"));
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    @Test
    public void testUnrequestedUnknown() {
        AbstractParameterTool parameter = createParameterToolFromArgs(new String[] {});
        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());

        Assertions.assertTrue(parameter.getBoolean("boolean", true));
        Assertions.assertEquals(0, parameter.getByte("byte", (byte) 0));
        Assertions.assertEquals(0, parameter.getShort("short", (short) 0));
        Assertions.assertEquals(0, parameter.getInt("int", 0));
        Assertions.assertEquals(0, parameter.getLong("long", 0));
        Assertions.assertEquals(0, parameter.getFloat("float", 0), 0.00001);
        Assertions.assertEquals(0, parameter.getDouble("double", 0), 0.00001);
        Assertions.assertEquals("0", parameter.get("string", "0"));

        Assertions.assertEquals(Collections.emptySet(), parameter.getUnrequestedParameters());
    }

    protected AbstractParameterTool createParameterToolFromArgs(String[] args) {
        return ParameterTool.fromArgs(args);
    }

    protected static <T> Set<T> createHashSet(T... elements) {
        Set<T> set = new HashSet<>();
        for (T element : elements) {
            set.add(element);
        }
        return set;
    }

    protected void validate(AbstractParameterTool parameter) {
        ClosureCleaner.ensureSerializable(parameter);
        internalValidate(parameter);

        // -------- test behaviour after serialization ------------
        try {
            byte[] b = InstantiationUtil.serializeObject(parameter);
            final AbstractParameterTool copy =
                    InstantiationUtil.deserializeObject(b, getClass().getClassLoader());
            internalValidate(copy);
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private void internalValidate(AbstractParameterTool parameter) {
        Assertions.assertEquals("myInput", parameter.getRequired("input"));
        Assertions.assertEquals("myDefaultValue", parameter.get("output", "myDefaultValue"));
        Assertions.assertNull(parameter.get("whatever"));
        Assertions.assertEquals(15L, parameter.getLong("expectedCount", -1L));
        Assertions.assertTrue(parameter.getBoolean("thisIsUseful", true));
        Assertions.assertEquals(42, parameter.getByte("myDefaultByte", (byte) 42));
        Assertions.assertEquals(42, parameter.getShort("myDefaultShort", (short) 42));

        if (parameter instanceof ParameterTool) {
            ParameterTool parameterTool = (ParameterTool) parameter;
            final Configuration config = parameterTool.getConfiguration();
            Assertions.assertEquals(15L, config.getLong("expectedCount", -1L));

            final Properties props = parameterTool.getProperties();
            Assertions.assertEquals("myInput", props.getProperty("input"));

            // -------- test the default file creation ------------
            try {
                final String pathToFile = tmp.newFile().getAbsolutePath();
                parameterTool.createPropertiesFile(pathToFile);
                final Properties defaultProps = new Properties();
                try (FileInputStream fis = new FileInputStream(pathToFile)) {
                    defaultProps.load(fis);
                }

                Assertions.assertEquals("myDefaultValue", defaultProps.get("output"));
                Assertions.assertEquals("-1", defaultProps.get("expectedCount"));
                Assertions.assertTrue(defaultProps.containsKey("input"));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else if (parameter instanceof MultipleParameterTool) {
            MultipleParameterTool multipleParameterTool = (MultipleParameterTool) parameter;
            List<String> multiValues = Arrays.asList("multiValue1", "multiValue2");
            Assertions.assertEquals(multiValues, multipleParameterTool.getMultiParameter("multi"));

            Assertions.assertEquals(multiValues, multipleParameterTool.toMultiMap().get("multi"));

            // The last value is used.
            Assertions.assertEquals("multiValue2", multipleParameterTool.toMap().get("multi"));
        }
    }
}
