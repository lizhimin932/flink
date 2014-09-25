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

package org.apache.flink.runtime.execution.librarycache;

import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.blob.BlobCache;
import org.apache.flink.runtime.blob.BlobClient;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.jobgraph.JobID;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class LibraryCacheManagerTest {

	@Test
	public void testLibraryCacheManagerCleanup(){
		Configuration config = new Configuration();

		config.setLong(ConfigConstants.LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL, 1);
		GlobalConfiguration.includeConfiguration(config);

		JobID jid = new JobID();
		List<BlobKey> keys = new ArrayList<BlobKey>();
		BlobServer server = null;

		final byte[] buf = new byte[128];

		try {
			server = new BlobServer();
			InetSocketAddress blobSocketAddress = new InetSocketAddress(server.getServerPort());
			BlobClient bc = new BlobClient(blobSocketAddress);

			keys.add(bc.put(buf));
			buf[0] += 1;
			keys.add(bc.put(buf));

			LibraryCacheManager.setBlobServerAddress(blobSocketAddress);
			LibraryCacheManager.register(jid, keys);

			List<URL> urls = new ArrayList<URL>();

			for(BlobKey key: keys){
				urls.add(BlobCache.getURL(blobSocketAddress, key));
			}

			assertEquals(2, urls.size());
			urls.clear();

			LibraryCacheManager.unregister(jid);

			Thread.sleep(1500);

			int caughtExceptions = 0;

			for (BlobKey key : keys) {
				// the blob cache should no longer contain the files
				try {
					urls.add(BlobCache.getURL(blobSocketAddress, key));
				} catch (IOException ioe) {
					caughtExceptions++;
				}
			}

			assertEquals(2, caughtExceptions);
		}catch(Exception e){
			e.printStackTrace();
			fail(e.getMessage());
		}finally{
			if(server != null){
				server.shutDown();
			}
		}
	}
}
