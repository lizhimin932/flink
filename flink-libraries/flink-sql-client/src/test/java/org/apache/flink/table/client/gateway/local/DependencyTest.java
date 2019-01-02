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

package org.apache.flink.table.client.gateway.local;

import org.apache.flink.client.cli.DefaultCLI;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.catalog.ExternalCatalog;
import org.apache.flink.table.catalog.ExternalCatalogTable;
import org.apache.flink.table.catalog.InMemoryExternalCatalog;
import org.apache.flink.table.client.config.Environment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.utils.EnvironmentFileUtil;
import org.apache.flink.table.client.gateway.utils.TestTableSinkFactoryBase;
import org.apache.flink.table.client.gateway.utils.TestTableSourceFactoryBase;
import org.apache.flink.table.descriptors.ConnectorDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.table.factories.ExternalCatalogFactory;

import org.junit.Test;

import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ExternalCatalogDescriptorValidator.CATALOG_TYPE;
import static org.junit.Assert.assertEquals;

/**
 * Dependency tests for {@link LocalExecutor}. Mainly for testing classloading of dependencies.
 */
public class DependencyTest {

	public static final String CATALOG_TYPE_VALUE = "DependencyTest";
	public static final String CONNECTOR_TYPE_VALUE = "DependencyTest";
	public static final String TEST_PROPERTY = "test-property";

	private static final String FACTORY_ENVIRONMENT_FILE = "test-sql-client-factory.yaml";
	private static final String TABLE_FACTORY_JAR_FILE = "table-factories-test-jar.jar";

	@Test
	public void testTableFactoryDiscovery() throws Exception {
		// create environment
		final Map<String, String> replaceVars = new HashMap<>();
		replaceVars.put("$VAR_CONNECTOR_TYPE", CONNECTOR_TYPE_VALUE);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY", TEST_PROPERTY);
		replaceVars.put("$VAR_CONNECTOR_PROPERTY_VALUE", "test-value");
		final Environment env = EnvironmentFileUtil.parseModified(FACTORY_ENVIRONMENT_FILE, replaceVars);

		// create executor with dependencies
		final URL dependency = Paths.get("target", TABLE_FACTORY_JAR_FILE).toUri().toURL();
		final LocalExecutor executor = new LocalExecutor(
			env,
			Collections.singletonList(dependency),
			new Configuration(),
			new DefaultCLI(new Configuration()));

		final SessionContext session = new SessionContext("test-session", new Environment());

		final List<String> actualTables = executor.listTables(session);
		final List<String> expectedTables = new ArrayList<>();
		expectedTables.add("TableNumber1");
		assertEquals(expectedTables, actualTables);

		final TableSchema result = executor.getTableSchema(session, "TableNumber1");
		final TableSchema expected = TableSchema.builder()
			.field("IntegerField1", Types.INT())
			.field("StringField1", Types.STRING())
			.field("rowtimeField", Types.SQL_TIMESTAMP())
			.build();

		assertEquals(expected, result);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Table source that can be discovered if classloading is correct.
	 */
	public static class TestTableSourceFactory extends TestTableSourceFactoryBase {

		public TestTableSourceFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * Table sink that can be discovered if classloading is correct.
	 */
	public static class TestTableSinkFactory extends TestTableSinkFactoryBase {

		public TestTableSinkFactory() {
			super(CONNECTOR_TYPE_VALUE, TEST_PROPERTY);
		}
	}

	/**
	 * External catalog that can be discovered if classloading is correct.
	 */
	public static class TestExternalCatalogFactory implements ExternalCatalogFactory {

		@Override
		public Map<String, String> requiredContext() {
			final Map<String, String> context = new HashMap<>();
			context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE);
			return context;
		}

		@Override
		public List<String> supportedProperties() {
			final List<String> properties = new ArrayList<>();
			properties.add(TEST_PROPERTY);
			return properties;
		}

		@Override
		public ExternalCatalog createExternalCatalog(Map<String, String> properties) {
			final DescriptorProperties params = new DescriptorProperties(true);
			params.putProperties(properties);
			return new TestExternalCatalog(params);
		}

		// --------------------------------------------------------------------------------------------

		/**
		 * Test catalog.
		 */
		public static class TestExternalCatalog extends InMemoryExternalCatalog {
			private final DescriptorProperties params;
			public TestExternalCatalog(DescriptorProperties params) {
				super("test");
				this.params = params;

				ExternalCatalogTable table1 = ExternalCatalogTable.builder(new TestConnectorDescriptor())
					.withSchema(Schema.apply())
					.inAppendMode()
					.asTableSourceAndSink();

				createTable("TableNumber1", table1, false);
			}
		}

		/**
		 * Test connector descriptor for {@link TestTableSourceFactory}.
		 */
		public static class TestConnectorDescriptor extends ConnectorDescriptor {
			public TestConnectorDescriptor() {
				super(CONNECTOR_TYPE_VALUE, 1, false);
			}

			@Override
			protected Map<String, String> toConnectorProperties() {
				final DescriptorProperties properties = new DescriptorProperties();
				properties.putString(TEST_PROPERTY, "test-value");
				return properties.asMap();
			}
		}
	}
}
