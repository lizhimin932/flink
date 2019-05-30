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

package org.apache.flink.table.catalog.hive;

import org.apache.flink.table.catalog.exceptions.CatalogException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Function;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

/**
 * Shim for Hive version 2.x.
 */
public class HiveShimV2 implements HiveShim {

	@Override
	public IMetaStoreClient getHiveMetastoreClient(HiveConf hiveConf) {
		try {
			Method method = RetryingMetaStoreClient.class.getMethod("getProxy", HiveConf.class, Boolean.TYPE);
			// getProxy is a static method
			return (IMetaStoreClient) method.invoke(null, hiveConf, true);
		} catch (Exception ex) {
			throw new CatalogException("Failed to create Hive Metastore client", ex);
		}
	}

	@Override
	public List<String> getViews(IMetaStoreClient client, String databaseName) throws UnknownDBException, TException {
		try {
			Method method = client.getClass().getMethod("getTables", String.class, String.class, TableType.class);
			return (List<String>) method.invoke(client, databaseName, null, TableType.VIRTUAL_VIEW);
		} catch (InvocationTargetException ite) {
			Throwable targetEx = ite.getTargetException();
			if (targetEx instanceof TException) {
				throw (TException) targetEx;
			} else {
				throw new CatalogException(String.format("Failed to get views for %s", databaseName), targetEx);
			}
		} catch (NoSuchMethodException | IllegalAccessException e) {
			throw new CatalogException(String.format("Failed to get views for %s", databaseName), e);
		}
	}

	@Override
	public Function getFunction(IMetaStoreClient client, String dbName, String functionName) throws NoSuchObjectException, TException {
		return client.getFunction(dbName, functionName);
	}
}
