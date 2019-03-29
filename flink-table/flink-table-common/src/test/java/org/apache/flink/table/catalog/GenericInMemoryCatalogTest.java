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

package org.apache.flink.table.catalog;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.exceptions.DatabaseAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotEmptyException;
import org.apache.flink.table.catalog.exceptions.DatabaseNotExistException;
import org.apache.flink.table.catalog.exceptions.TableAlreadyExistException;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test for GenericInMemoryCatalog.
 */
public class GenericInMemoryCatalogTest {
	private static final String IS_STREAMING = "is_streaming";

	private final String testCatalogName = "test-catalog";
	private final String db1 = "db1";
	private final String db2 = "db2";

	private final String t1 = "t1";
	private final String t2 = "t2";
	private final ObjectPath path1 = new ObjectPath(db1, t1);
	private final ObjectPath path2 = new ObjectPath(db2, t2);
	private final ObjectPath path3 = new ObjectPath(db1, t2);
	private final ObjectPath nonExistDbPath = ObjectPath.fromString("non.exist");
	private final ObjectPath nonExistObjectPath = ObjectPath.fromString("db1.nonexist");

	private static final String TEST_COMMENT = "test comment";

	private static ReadableWritableCatalog catalog;

	public String getTableType() {
		return "csv";
	}

	@Before
	public void setUp() {
		catalog = new GenericInMemoryCatalog(db1);
	}

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@After
	public void close() {
		if (catalog.tableExists(path1)) {
			catalog.dropTable(path1, true);
		}
		if (catalog.tableExists(path2)) {
			catalog.dropTable(path2, true);
		}
		if (catalog.tableExists(path3)) {
			catalog.dropTable(path3, true);
		}
		if (catalog.databaseExists(db1)) {
			catalog.dropDatabase(db1, true);
		}
		if (catalog.databaseExists(db2)) {
			catalog.dropDatabase(db2, true);
		}
	}

	@AfterClass
	public static void clean() throws IOException {
		catalog.close();
	}

	// ------ tables ------

	@Test
	public void testCreateTable_Streaming() {
		catalog.createDatabase(db1, createDb(), false);
		GenericCatalogTable table = createStreamingTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testCreateTable_Batch() {
		catalog.createDatabase(db1, createDb(), false);

		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));

		List<String> tables = catalog.listTables(db1);

		assertEquals(1, tables.size());
		assertEquals(path1.getObjectName(), tables.get(0));

		catalog.dropTable(path1, false);
	}

	@Test
	public void testCreateTable_DatabaseNotExistException() {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createTable(nonExistObjectPath, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1,  CatalogTestUtil.createTable(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.createTable(path1, createTable(), false);
	}

	@Test
	public void testCreateTable_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		GenericCatalogTable table =  CatalogTestUtil.createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.createTable(path1, createAnotherTable(), true);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));
	}

	@Test
	public void testGetTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testGetTable_TableNotExistException_NoDb() {
		exception.expect(TableNotExistException.class);
		catalog.getTable(nonExistObjectPath);
	}

	@Test
	public void testDropTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testDropTable_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.dropTable(nonExistDbPath, false);
	}

	@Test
	public void testDropTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.dropTable(nonExistObjectPath, true);
	}

	@Test
	public void testAlterTable() {
		catalog.createDatabase(db1, createDb(), false);

		// Non-partitioned table
		GenericCatalogTable table = CatalogTestUtil.createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));

		GenericCatalogTable newTable = createAnotherTable();
		catalog.alterTable(path1, newTable, false);

		assertNotEquals(table, catalog.getTable(path1));
		CatalogTestUtil.compare(newTable, (GenericCatalogTable) catalog.getTable(path1));

		catalog.dropTable(path1, false);
	}

	@Test
	public void testAlterTable_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterTable_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterTable(nonExistObjectPath, createTable(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testRenameTable() {
		catalog.createDatabase(db1, createDb(), false);
		GenericCatalogTable table = createTable();
		catalog.createTable(path1, table, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path1));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.compare(table, (GenericCatalogTable) catalog.getTable(path3));
		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testRenameTable_TableNotExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(TableNotExistException.class);
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testRenameTable_TableNotExistException_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.renameTable(path1, t2, true);
	}

	@Test
	public void testRenameTable_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable();
		catalog.createTable(path1, table, false);
		catalog.createTable(path3, createAnotherTable(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.renameTable(path1, t2, false);
	}

	@Test
	public void testTableExists() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		catalog.createTable(path1, createTable(), false);

		assertTrue(catalog.tableExists(path1));
	}

	// ------ views ------

	@Test
	public void testCreateView() {
		catalog.createDatabase(db1, createDb(), false);

		assertFalse(catalog.tableExists(path1));

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.compare(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testCreateView_DatabaseNotExistException() {
		assertFalse(catalog.databaseExists(db1));

		exception.expect(DatabaseNotExistException.class);
		catalog.createView(nonExistObjectPath, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createView(path1, createView(), false);

		exception.expect(TableAlreadyExistException.class);
		catalog.createView(path1, createView(), false);
	}

	@Test
	public void testCreateView_TableAlreadyExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.compare(view, (GenericCatalogView) catalog.getTable(path1));

		catalog.createView(path1, createAnotherView(), true);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.compare(view, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testDropView() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createView(path1, createView(), false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
	}

	@Test
	public void testAlterView() {
		catalog.createDatabase(db1, createDb(), false);

		CatalogView view = createView();
		catalog.createView(path1, view, false);

		CatalogTestUtil.compare(view, (GenericCatalogView) catalog.getTable(path1));

		CatalogView newView = createAnotherView();
		catalog.alterTable(path1, newView, false);

		assertTrue(catalog.getTable(path1) instanceof CatalogView);
		CatalogTestUtil.compare(newView, (GenericCatalogView) catalog.getTable(path1));
	}

	@Test
	public void testAlterView_TableNotExistException() {
		exception.expect(TableNotExistException.class);
		catalog.alterTable(nonExistDbPath, createTable(), false);
	}

	@Test
	public void testAlterView_TableNotExist_ignored() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.alterView(nonExistObjectPath, createView(), true);

		assertFalse(catalog.tableExists(nonExistObjectPath));
	}

	@Test
	public void testListView() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listTables(db1).isEmpty());

		catalog.createView(path1, createView(), false);
		catalog.createTable(path3, createTable(), false);

		assertEquals(2, catalog.listTables(db1).size());
		assertEquals(new HashSet<>(Arrays.asList(path1.getObjectName(), path3.getObjectName())),
			new HashSet<>(catalog.listTables(db1)));
		assertEquals(Arrays.asList(path1.getObjectName()), catalog.listViews(db1));
	}

	// ------ databases ------

	@Test
	public void testCreateDb() {
		catalog.createDatabase(db2, createDb(), false);

		assertEquals(2, catalog.listDatabases().size());
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExistException() {
		catalog.createDatabase(db1, createDb(), false);

		exception.expect(DatabaseAlreadyExistException.class);
		catalog.createDatabase(db1, createDb(), false);
	}

	@Test
	public void testCreateDb_DatabaseAlreadyExist_ignored() {
		CatalogDatabase cd1 = createDb();
		catalog.createDatabase(db1, cd1, false);
		List<String> dbs = catalog.listDatabases();

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));

		catalog.createDatabase(db1, createAnotherDb(), true);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(cd1.getProperties().entrySet()));
		assertEquals(2, dbs.size());
		assertEquals(new HashSet<>(Arrays.asList(db1, catalog.getDefaultDatabaseName())), new HashSet<>(dbs));
	}

	@Test
	public void testGetDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.getDatabase("nonexistent");
	}

	@Test
	public void testDropDb() {
		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.listDatabases().contains(db1));

		catalog.dropDatabase(db1, false);

		assertFalse(catalog.listDatabases().contains(db1));
	}

	@Test
	public void testDropDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.dropDatabase(db1, false);
	}

	@Test
	public void testDropDb_DatabaseNotExist_Ignore() {
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testDropDb_databaseIsNotEmpty() {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createTable(), false);

		exception.expect(DatabaseNotEmptyException.class);
		catalog.dropDatabase(db1, true);
	}

	@Test
	public void testAlterDb() {
		CatalogDatabase db = createDb();
		catalog.createDatabase(db1, db, false);

		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));

		CatalogDatabase newDb = createAnotherDb();
		catalog.alterDatabase(db1, newDb, false);

		assertFalse(catalog.getDatabase(db1).getProperties().entrySet().containsAll(db.getProperties().entrySet()));
		assertTrue(catalog.getDatabase(db1).getProperties().entrySet().containsAll(newDb.getProperties().entrySet()));
	}

	@Test
	public void testAlterDb_DatabaseNotExistException() {
		exception.expect(DatabaseNotExistException.class);
		catalog.alterDatabase("nonexistent", createDb(), false);
	}

	@Test
	public void testAlterDb_DatabaseNotExist_ignored() {
		catalog.alterDatabase("nonexistent", createDb(), true);

		assertFalse(catalog.databaseExists("nonexistent"));
	}

	@Test
	public void testDbExists() {
		assertFalse(catalog.databaseExists("nonexistent"));

		catalog.createDatabase(db1, createDb(), false);

		assertTrue(catalog.databaseExists(db1));
	}

	// ------ utilities ------

	private GenericCatalogTable createStreamingTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getStreamingTableProperties());
	}

	private GenericCatalogTable createTable() {
		return CatalogTestUtil.createTable(
			createTableSchema(),
			getBatchTableProperties());
	}

	private GenericCatalogTable createAnotherTable() {
		return CatalogTestUtil.createTable(
			createAnotherTableSchema(),
			getBatchTableProperties());
	}

	private CatalogDatabase createDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k1", "v1");
		}}, TEST_COMMENT);
	}

	private Map<String, String> getBatchTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "false");
		}};
	}

	private Map<String, String> getStreamingTableProperties() {
		return new HashMap<String, String>() {{
			put(IS_STREAMING, "true");
		}};
	}

	private CatalogDatabase createAnotherDb() {
		return new GenericCatalogDatabase(new HashMap<String, String>() {{
			put("k2", "v2");
		}}, "this is another database.");
	}

	private TableSchema createTableSchema() {
		return new TableSchema(
			new String[] {"first", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.INT_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
			}
		);
	}

	private TableSchema createAnotherTableSchema() {
		return new TableSchema(
			new String[] {"first2", "second", "third"},
			new TypeInformation[] {
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO,
				BasicTypeInfo.STRING_TYPE_INFO
			}
		);
	}

	private CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", testCatalogName, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	private CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", testCatalogName, path2.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

}
