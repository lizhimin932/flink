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

package org.apache.flink.runtime.blob;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.util.TestLogger;

import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.InetSocketAddress;

import static org.apache.flink.runtime.blob.BlobKey.BlobType.PERMANENT_BLOB;
import static org.apache.flink.runtime.blob.BlobKey.BlobType.TRANSIENT_BLOB;
import static org.apache.flink.runtime.blob.BlobServerPutTest.put;
import static org.apache.flink.runtime.blob.BlobServerPutTest.verifyContents;
import static org.junit.Assert.fail;

/** Unit tests for the blob cache retrying the connection to the server. */
public class BlobCacheRetriesTest extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    /**
     * A test where the connection fails twice and then the get operation succeeds (job-unrelated
     * blob).
     */
    @Test
    public void testBlobFetchRetries() throws IOException {
        testBlobFetchRetries(new VoidBlobStore(), null, TRANSIENT_BLOB);
    }

    /**
     * A test where the connection fails twice and then the get operation succeeds (job-related
     * blob).
     */
    @Test
    public void testBlobForJobFetchRetries() throws IOException {
        testBlobFetchRetries(new VoidBlobStore(), new JobID(), TRANSIENT_BLOB);
    }

    /**
     * A test where the connection fails twice and then the get operation succeeds (with high
     * availability set, job-related job).
     */
    @Test
    public void testBlobFetchRetriesHa() throws IOException {
        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.newFolder().getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            testBlobFetchRetries(blobStoreService, new JobID(), PERMANENT_BLOB);
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }

    /**
     * A test where the BlobCache must use the BlobServer and the connection fails twice and then
     * the get operation succeeds.
     *
     * @param blobType whether the BLOB should become permanent or transient
     */
    private static void testBlobFetchRetries(
            final BlobStore blobStore, @Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {

        final Configuration configuration = new Configuration();

        final byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

        try (BlobServer server =
                        new TestingFailingBlobServer(
                                configuration, TEMPORARY_FOLDER.newFolder(), blobStore, 2);
                BlobCacheService cache =
                        new BlobCacheService(
                                configuration,
                                TEMPORARY_FOLDER.newFolder(),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // upload some blob
            final BlobKey key = put(server, jobId, data, blobType);

            // trigger a download - it should fail the first two times, but retry, and succeed
            // eventually
            verifyContents(cache, jobId, key, data);
        }
    }

    /**
     * A test where the connection fails too often and eventually fails the GET request
     * (job-unrelated blob).
     */
    @Test
    public void testBlobNoJobFetchWithTooManyFailures() throws IOException {
        testBlobFetchWithTooManyFailures(new VoidBlobStore(), null, TRANSIENT_BLOB);
    }

    /**
     * A test where the connection fails too often and eventually fails the GET request (job-related
     * blob).
     */
    @Test
    public void testBlobForJobFetchWithTooManyFailures() throws IOException {
        testBlobFetchWithTooManyFailures(new VoidBlobStore(), new JobID(), TRANSIENT_BLOB);
    }

    /**
     * A test where the connection fails too often and eventually fails the GET request (with high
     * availability set, job-related blob).
     */
    @Test
    public void testBlobForJobFetchWithTooManyFailuresHa() throws IOException {
        final Configuration config = new Configuration();
        config.setString(HighAvailabilityOptions.HA_MODE, "ZOOKEEPER");
        config.setString(
                HighAvailabilityOptions.HA_STORAGE_PATH, TEMPORARY_FOLDER.getRoot().getPath());

        BlobStoreService blobStoreService = null;

        try {
            blobStoreService = BlobUtils.createBlobStoreFromConfig(config);

            testBlobFetchWithTooManyFailures(blobStoreService, new JobID(), PERMANENT_BLOB);
        } finally {
            if (blobStoreService != null) {
                blobStoreService.closeAndCleanupAllData();
            }
        }
    }

    /**
     * A test where the BlobCache must use the BlobServer and the connection fails too often which
     * eventually fails the GET request.
     *
     * @param blobType whether the BLOB should become permanent or transient
     */
    private static void testBlobFetchWithTooManyFailures(
            final BlobStore blobStore, @Nullable final JobID jobId, BlobKey.BlobType blobType)
            throws IOException {
        final Configuration configuration = new Configuration();

        final byte[] data = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 0};

        try (BlobServer server =
                        new TestingFailingBlobServer(
                                configuration, TEMPORARY_FOLDER.newFolder(), blobStore, 0, 10);
                BlobCacheService cache =
                        new BlobCacheService(
                                configuration,
                                TEMPORARY_FOLDER.newFolder(),
                                new VoidBlobStore(),
                                new InetSocketAddress("localhost", server.getPort()))) {

            server.start();

            // upload some blob
            final BlobKey key = put(server, jobId, data, blobType);

            // trigger a download - it should fail eventually
            try {
                verifyContents(cache, jobId, key, data);
                fail("This should fail");
            } catch (IOException e) {
                // as we expected
            }
        }
    }
}
