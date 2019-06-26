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

import org.apache.flink.table.catalog.exceptions.CatalogException;
import org.apache.flink.table.catalog.stats.CatalogColumnStatistics;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBase;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBinary;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataBoolean;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDate;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataDouble;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataLong;
import org.apache.flink.table.catalog.stats.CatalogColumnStatisticsDataString;
import org.apache.flink.table.catalog.stats.CatalogTableStatistics;
import org.apache.flink.table.catalog.stats.Date;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test for GenericInMemoryCatalog.
 */
public class GenericInMemoryCatalogTest extends CatalogTestBase {

	@BeforeClass
	public static void init() {
		catalog = new GenericInMemoryCatalog(TEST_CATALOG_NAME);
		catalog.open();
	}

	// ------ tables ------

	@Test
	public void testDropTable_partitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(path1), false);
		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
		catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

		assertTrue(catalog.tableExists(path1));

		catalog.dropTable(path1, false);

		assertFalse(catalog.tableExists(path1));
		assertFalse(catalog.partitionExists(path1, catalogPartitionSpec));
	}

	@Test
	public void testRenameTable_partitionedTable() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createPartitionedTable(path1);
		catalog.createTable(path1, table, false);
		CatalogPartition catalogPartition = createPartition();
		CatalogPartitionSpec catalogPartitionSpec = createPartitionSpec();
		catalog.createPartition(path1, catalogPartitionSpec, catalogPartition, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path1));
		assertTrue(catalog.partitionExists(path1, catalogPartitionSpec));

		catalog.renameTable(path1, t2, false);

		CatalogTestUtil.checkEquals(table, (CatalogTable) catalog.getTable(path3));
		assertTrue(catalog.partitionExists(path3, catalogPartitionSpec));
		assertFalse(catalog.tableExists(path1));
		assertFalse(catalog.partitionExists(path1, catalogPartitionSpec));
	}

	// ------ partitions ------

	@Test
	public void testAlterPartition_differentTypedPartition() throws Exception {
		catalog.createDatabase(db1, createDb(), false);
		catalog.createTable(path1, createPartitionedTable(path1), false);

		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		CatalogPartition partition = createPartition();
		catalog.createPartition(path1, partitionSpec, partition, false);

		exception.expect(CatalogException.class);
		exception.expectMessage(
			String.format("Partition types don't match. " +
				"Existing partition is '%s' and " +
				"new partition is 'org.apache.flink.table.catalog.CatalogTest$TestPartition'.",
				partition.getClass().getName()));
		catalog.alterPartition(path1, partitionSpec, new TestPartition(), false);
	}

	// ------ statistics ------

	@Test
	public void testStatistics() throws Exception {
		// Table related
		catalog.createDatabase(db1, createDb(), false);
		CatalogTable table = createTable(path1);
		catalog.createTable(path1, table, false);

		CatalogTestUtil.checkEquals(catalog.getTableStatistics(path1), CatalogTableStatistics.UNKNOWN);
		CatalogTestUtil.checkEquals(catalog.getTableColumnStatistics(path1), CatalogColumnStatistics.UNKNOWN);

		CatalogTableStatistics tableStatistics = new CatalogTableStatistics(5, 2, 100, 575);
		catalog.alterTableStatistics(path1, tableStatistics, false);
		CatalogTestUtil.checkEquals(tableStatistics, catalog.getTableStatistics(path1));
		CatalogColumnStatistics columnStatistics = createColumnStats();
		catalog.alterTableColumnStatistics(path1, columnStatistics, false);
		CatalogTestUtil.checkEquals(columnStatistics, catalog.getTableColumnStatistics(path1));

		// Partition related
		catalog.createDatabase(db2, createDb(), false);
		CatalogTable table2 = createPartitionedTable(path2);
		catalog.createTable(path2, table2, false);
		CatalogPartitionSpec partitionSpec = createPartitionSpec();
		catalog.createPartition(path2, partitionSpec, createPartition(), false);

		CatalogTestUtil.checkEquals(catalog.getPartitionStatistics(path2, partitionSpec), CatalogTableStatistics.UNKNOWN);
		CatalogTestUtil.checkEquals(catalog.getPartitionColumnStatistics(path2, partitionSpec), CatalogColumnStatistics.UNKNOWN);

		catalog.alterPartitionStatistics(path2, partitionSpec, tableStatistics, false);
		CatalogTestUtil.checkEquals(tableStatistics, catalog.getPartitionStatistics(path2, partitionSpec));
		catalog.alterPartitionColumnStatistics(path2, partitionSpec, columnStatistics, false);
		CatalogTestUtil.checkEquals(columnStatistics, catalog.getPartitionColumnStatistics(path2, partitionSpec));

		// Clean up
		catalog.dropTable(path1, false);
		catalog.dropDatabase(db1, false);
		catalog.dropTable(path2, false);
		catalog.dropDatabase(db2, false);
	}

	// ------ utilities ------

	@Override
	protected boolean isGeneric() {
		return true;
	}

	@Override
	public CatalogTable createStreamingTable(ObjectPath tablePath) {
		return new CatalogTableImpl(tablePath,
			createTableSchema(),
			getStreamingTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createTable(ObjectPath tablePath) {
		return new CatalogTableImpl(tablePath,
			createTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherTable(ObjectPath tablePath) {
		return new CatalogTableImpl(tablePath,
			createAnotherTableSchema(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createPartitionedTable(ObjectPath tablePath) {
		return new CatalogTableImpl(tablePath,
			createTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogTable createAnotherPartitionedTable(ObjectPath tablePath) {
		return new CatalogTableImpl(tablePath,
			createAnotherTableSchema(),
			createPartitionKeys(),
			getBatchTableProperties(),
			TEST_COMMENT);
	}

	@Override
	public CatalogPartition createPartition() {
		return new GenericCatalogPartition(getBatchTableProperties(), "Generic batch table");
	}

	@Override
	public CatalogView createView() {
		return new GenericCatalogView(
			String.format("select * from %s", t1),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path1.getFullName()),
			createTableSchema(),
			new HashMap<>(),
			"This is a view");
	}

	@Override
	public CatalogView createAnotherView() {
		return new GenericCatalogView(
			String.format("select * from %s", t2),
			String.format("select * from %s.%s", TEST_CATALOG_NAME, path2.getFullName()),
			createAnotherTableSchema(),
			new HashMap<>(),
			"This is another view");
	}

	private CatalogColumnStatistics createColumnStats() {
		CatalogColumnStatisticsDataBoolean booleanColStats = new CatalogColumnStatisticsDataBoolean(55L, 45L, 5L);
		CatalogColumnStatisticsDataLong longColStats = new CatalogColumnStatisticsDataLong(-123L, 763322L, 23L, 79L);
		CatalogColumnStatisticsDataString stringColStats = new CatalogColumnStatisticsDataString(152L, 43.5D, 20L, 0L);
		CatalogColumnStatisticsDataDate dateColStats = new CatalogColumnStatisticsDataDate(new Date(71L),
			new Date(17923L), 1321, 0L);
		CatalogColumnStatisticsDataDouble doubleColStats = new CatalogColumnStatisticsDataDouble(-123.35D, 7633.22D, 23L, 79L);
		CatalogColumnStatisticsDataBinary binaryColStats = new CatalogColumnStatisticsDataBinary(755L, 43.5D, 20L);
		Map<String, CatalogColumnStatisticsDataBase> colStatsMap = new HashMap<>(6);
		colStatsMap.put("b1", booleanColStats);
		colStatsMap.put("l2", longColStats);
		colStatsMap.put("s3", stringColStats);
		colStatsMap.put("d4", dateColStats);
		colStatsMap.put("dd5", doubleColStats);
		colStatsMap.put("bb6", binaryColStats);
		return new CatalogColumnStatistics(colStatsMap);
	}

	@Override
	protected CatalogFunction createFunction() {
		return new GenericCatalogFunction(MyScalarFunction.class.getName(), new HashMap<>());
	}

	@Override
	protected CatalogFunction createAnotherFunction() {
		return new GenericCatalogFunction(MyOtherScalarFunction.class.getName(), new HashMap<>());
	}

	/**
	 * Test UDF.
	 */
	public static class MyScalarFunction extends ScalarFunction {
		public Integer eval(Integer i) {
			return i + 1;
		}
	}

	/**
	 * Test UDF.
	 */
	public static class MyOtherScalarFunction extends ScalarFunction {
		public String eval(Integer i) {
			return String.valueOf(i);
		}
	}

}
