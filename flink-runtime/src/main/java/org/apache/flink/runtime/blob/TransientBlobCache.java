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
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides access to transient BLOB files stored at the {@link BlobServer}.
 *
 * TODO: currently, this is still cache-based with local copies - make this truly transient, i.e. return file streams with no local copy
 */
public class TransientBlobCache implements TransientBlobService {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(BlobCache.class);

	private final InetSocketAddress serverAddress;

	/**
	 * Root directory for local file storage
	 */
	private final File storageDir;

	private final AtomicBoolean shutdownRequested = new AtomicBoolean();

	/** Shutdown hook thread to ensure deletion of the storage directory. */
	private final Thread shutdownHook;

	/** The number of retries when the transfer fails */
	private final int numFetchRetries;

	/** Configuration for the blob client like ssl parameters required to connect to the blob server */
	private final Configuration blobClientConfig;

	/**
	 * Instantiates a new BLOB cache.
	 *
	 * @param serverAddress
	 * 		address of the {@link BlobServer} to use for fetching files from
	 * @param blobClientConfig
	 * 		global configuration
	 * @param blobView
	 * 		(distributed) blob store file system to retrieve files from first
	 *
	 * @throws IOException
	 * 		thrown if the (local or distributed) file storage cannot be created or is not usable
	 */
	public TransientBlobCache(
			final InetSocketAddress serverAddress,
			final Configuration blobClientConfig,
			final BlobView blobView) throws IOException {

		this.serverAddress = checkNotNull(serverAddress);
		this.blobClientConfig = checkNotNull(blobClientConfig);

		// configure and create the storage directory
		String storageDirectory = blobClientConfig.getString(BlobServerOptions.STORAGE_DIRECTORY);
		this.storageDir = BlobUtils.initLocalStorageDirectory(storageDirectory);
		LOG.info("Created BLOB cache storage directory " + storageDir);

		// configure the number of fetch retries
		final int fetchRetries = blobClientConfig.getInteger(BlobServerOptions.FETCH_RETRIES);
		if (fetchRetries >= 0) {
			this.numFetchRetries = fetchRetries;
		} else {
			LOG.warn("Invalid value for {}. System will attempt no retires on failed fetches of BLOBs.",
				BlobServerOptions.FETCH_RETRIES.key());
			this.numFetchRetries = 0;
		}

		// Add shutdown hook to delete storage directory
		shutdownHook = BlobUtils.addShutdownHook(this, LOG);
	}

	@Override
	public File getFile(BlobKey key) throws IOException {
		return getFileInternal(null, key);
	}

	@Override
	public File getFile(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		return getFileInternal(jobId, key);
	}

	/**
	 * Returns local copy of the file for the BLOB with the given key.
	 * <p>
	 * The method will first attempt to serve the BLOB from its local cache. If the BLOB is not in
	 * the cache, the method will try to download it from this cache's BLOB server.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param requiredBlob
	 * 		The key of the desired BLOB.
	 *
	 * @return file referring to the local storage location of the BLOB.
	 *
	 * @throws IOException
	 * 		Thrown if an I/O error occurs while downloading the BLOBs from the BLOB server.
	 */
	private File getFileInternal(@Nullable JobID jobId, BlobKey requiredBlob) throws IOException {
		checkArgument(requiredBlob != null, "BLOB key cannot be null.");

		final File localJarFile = BlobUtils.getStorageLocation(storageDir, jobId, requiredBlob);

		if (localJarFile.exists()) {
			return localJarFile;
		}

		// download from the BlobServer directly
		return BlobClient.downloadFromBlobServer(jobId, requiredBlob, localJarFile, serverAddress,
			blobClientConfig,
			numFetchRetries);
	}

	@Override
	public BlobKey put(byte[] value) throws IOException {
		return null;
	}

	@Override
	public BlobKey put(JobID jobId, byte[] value) throws IOException {
		checkNotNull(jobId);
		return null;
	}

	@Override
	public BlobKey put(InputStream inputStream) throws IOException {
		return null;
	}

	@Override
	public BlobKey put(JobID jobId, InputStream inputStream) throws IOException {
		checkNotNull(jobId);
		return null;
	}

	@Override
	public void delete(BlobKey key) throws IOException {
		deleteInternal(null, key);
	}

	@Override
	public void delete(JobID jobId, BlobKey key) throws IOException {
		checkNotNull(jobId);
		deleteInternal(jobId, key);
	}

	/**
	 * Deletes the file associated with the blob key in this BLOB cache.
	 *
	 * @param jobId
	 * 		ID of the job this blob belongs to (or <tt>null</tt> if job-unrelated)
	 * @param key
	 * 		blob key associated with the file to be deleted
	 *
	 * @throws IOException
	 */
	private void deleteInternal(@Nullable JobID jobId, BlobKey key) throws IOException {

		// delete locally
		final File localFile = BlobUtils.getStorageLocation(storageDir, jobId, key);

		if (!localFile.delete() && localFile.exists()) {
			LOG.warn("Failed to delete locally cached BLOB {} at {}", key,
				localFile.getAbsolutePath());
		}

		// then delete on the BLOB server
		try (BlobClient bc = createClient()) {
			bc.deleteInternal(jobId, key);
		}
	}

	public File getStorageDir() {
		return this.storageDir;
	}

	public int getPort() {
		return serverAddress.getPort();
	}

	private BlobClient createClient() throws IOException {
		return new BlobClient(serverAddress, blobClientConfig);
	}

	@Override
	public void close() throws IOException {
		if (shutdownRequested.compareAndSet(false, true)) {
			LOG.info("Shutting down BlobCache");

			// Clean up the storage directory
			try {
				FileUtils.deleteDirectory(storageDir);
			} finally {
				// Remove shutdown hook to prevent resource leaks, unless this is invoked by the shutdown hook itself
				if (shutdownHook != null && shutdownHook != Thread.currentThread()) {
					try {
						Runtime.getRuntime().removeShutdownHook(shutdownHook);
					} catch (IllegalStateException e) {
						// race, JVM is in shutdown already, we can safely ignore this
					} catch (Throwable t) {
						LOG.warn("Exception while unregistering BLOB cache's cleanup shutdown hook.");
					}
				}
			}
		}
	}
}
