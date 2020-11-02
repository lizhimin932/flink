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

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorageFactory;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorageFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.DynamicCodeLoadingException;

import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This test validates that checkpoint storage is properly
 * loaded from configuration.
 */
public class CheckpointStorageLoadingTest {

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	private final ClassLoader cl = getClass().getClassLoader();

	@Test
	public void testNoCheckpointStorageDefined() throws Exception {
		assertNull(CheckpointStorageLoader.loadCheckpointStorageFromConfig(new Configuration(), cl, null));
	}

	@Test
	public void testLegacyStateBackendTakesPrecedence() throws Exception {
		StateBackend legacy = new LegacyStateBackend();
		CheckpointStorage storage = new MockStorage();

		CheckpointStorage configured = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(
				storage, null, legacy, new Configuration(), cl, null);

		Assert.assertEquals("Legacy state backends should always take precedence", legacy, configured);
	}

	@Test
	public void testModernStateBackendDoesNotTakePrecedence() throws Exception {
		StateBackend legacy = new ModernStateBackend();
		CheckpointStorage storage = new MockStorage();

		CheckpointStorage configured = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(
				storage, null, legacy, new Configuration(), cl, null);

		Assert.assertEquals("Modern state backends should never take precedence", storage, configured);
	}

	@Test
	public void testLoadingFromFactory() throws Exception {
		final Configuration config = new Configuration();

		config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, WorkingFactory.class.getName());
		CheckpointStorage storage = CheckpointStorageLoader
				.fromApplicationOrConfigOrDefault(null,
						null,
						new ModernStateBackend(), config, cl, null);
		Assert.assertThat(storage, Matchers.instanceOf(MockStorage.class));
	}

	@Test
	public void testDefaultCheckpointStorage() throws Exception {
		CheckpointStorage storage1 = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(
				null,
				null,
				new ModernStateBackend(),
				new Configuration(),
				cl,
				null);

		Assert.assertThat(storage1, Matchers.instanceOf(JobManagerCheckpointStorage.class));

		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		Configuration config = new Configuration();
		config.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		CheckpointStorage storage2 = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(
				null,
				null,
				new ModernStateBackend(),
				config,
				cl,
				null);

		Assert.assertThat(storage2, Matchers.instanceOf(FileSystemCheckpointStorage.class));
	}

	@Test
	public void testLoadingFails() throws Exception {
		final Configuration config = new Configuration();

		config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, "does.not.exist");
		try {
			CheckpointStorageLoader.fromApplicationOrConfigOrDefault(null,
					null,
					new ModernStateBackend(), config, cl, null);
			Assert.fail("should fail with exception");
		} catch (DynamicCodeLoadingException e) {
			// expected
		}

		// try a class that is not a factory
		config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, java.io.File.class.getName());
		try {
			CheckpointStorageLoader.fromApplicationOrConfigOrDefault(null,
					null,
					new ModernStateBackend(), config, cl, null);
			Assert.fail("should fail with exception");
		} catch (DynamicCodeLoadingException e) {
			// expected
		}

		// try a factory that fails
		config.setString(CheckpointingOptions.CHECKPOINT_STORAGE, FailingFactory.class.getName());
		try {
			CheckpointStorageLoader.fromApplicationOrConfigOrDefault(null,
					null,
					new ModernStateBackend(), config, cl, null);
			Assert.fail("should fail with exception");
		} catch (IOException e) {
			// expected
		}
	}

	// ------------------------------------------------------------------------
	//  Job Manager Checkpoint Storage
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a job manager checkpoint storage from the cluster configuration.
	 */
	@Test
	public void testLoadJobManagerStorageNoParameters() throws Exception {
		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");

		final Configuration config2 = new Configuration();
		config2.set(CheckpointingOptions.CHECKPOINT_STORAGE, JobManagerCheckpointStorageFactory.class.getName());

		CheckpointStorage storage1 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config1, cl, null);
		CheckpointStorage storage2 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config2, cl, null);

		Assert.assertThat(storage1, Matchers.instanceOf(JobManagerCheckpointStorage.class));
		Assert.assertThat(storage2, Matchers.instanceOf(JobManagerCheckpointStorage.class));
	}

	/**
	 * Validates loading a job manager checkpoint storage with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadJobManagerStorageWithParameters() throws Exception {
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedSavepointPath = new Path(savepointDir);

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name

		final Configuration config1 = new Configuration();
		config1.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		final Configuration config2 = new Configuration();
		config2.set(CheckpointingOptions.CHECKPOINT_STORAGE, JobManagerCheckpointStorageFactory.class.getName());
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		CheckpointStorage storage1 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config1, cl, null);
		CheckpointStorage storage2 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config2, cl, null);

		Assert.assertThat(storage1, Matchers.instanceOf(JobManagerCheckpointStorage.class));
		Assert.assertThat(storage2, Matchers.instanceOf(JobManagerCheckpointStorage.class));

		assertEquals(expectedSavepointPath, ((JobManagerCheckpointStorage) storage1).getSavepointPath());
		assertEquals(expectedSavepointPath, ((JobManagerCheckpointStorage) storage2).getSavepointPath());
	}

	/**
	 * Validates taking the application-defined job manager checkpoint storage and adding additional
	 * parameters from the cluster configuration.
	 */
	@Test
	public void testConfigureJobManagerStorage() throws Exception {
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedSavepointPath = new Path(savepointDir);

		final int maxSize = 100;

		final Configuration config = new Configuration();
		config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem"); // check that this is not accidentally picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);

		CheckpointStorage storage = CheckpointStorageLoader
				.fromApplicationOrConfigOrDefault(new JobManagerCheckpointStorage(maxSize),
						null,
						new ModernStateBackend(), config, cl, null);
		assertTrue(storage instanceof JobManagerCheckpointStorage);
		JobManagerCheckpointStorage jmStorage = (JobManagerCheckpointStorage) storage;
		assertEquals(expectedSavepointPath, jmStorage.getSavepointPath());
		assertEquals(maxSize, jmStorage.getMaxStateSize());
	}

	/**
	 * Tests that job parameters take precedence over cluster configurations.
	 */
	@Test
	public void testConfigureJobManagerStorageWithParameters() throws Exception {
		final String savepointDirConfig = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDirJob = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedSavepointPath = new Path(savepointDirJob);

		final Configuration config = new Configuration();
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDirConfig);

		CheckpointStorage storage = CheckpointStorageLoader
				.fromApplicationOrConfigOrDefault(new JobManagerCheckpointStorage(),
						savepointDirJob,
						new ModernStateBackend(), config, cl, null);
		assertTrue(storage instanceof JobManagerCheckpointStorage);
		JobManagerCheckpointStorage jmStorage = (JobManagerCheckpointStorage) storage;
		assertEquals(expectedSavepointPath, jmStorage.getSavepointPath());
	}

	// ------------------------------------------------------------------------
	//  File System Checkpoint Storage
	// ------------------------------------------------------------------------

	/**
	 * Validates loading a file system checkpoint storage with additional parameters from the cluster configuration.
	 */
	@Test
	public void testLoadFileSystemCheckpointStorage() throws Exception {
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();
		final Path expectedCheckpointsPath = new Path(checkpointDir);
		final Path expectedSavepointsPath = new Path(savepointDir);
		final MemorySize threshold = MemorySize.parse("900kb");
		final int minWriteBufferSize = 1024;
		final boolean async = !CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue();

		// we configure with the explicit string (rather than AbstractStateBackend#X_STATE_BACKEND_NAME)
		// to guard against config-breaking changes of the name
		final Configuration config1 = new Configuration();
		config1.set(CheckpointingOptions.CHECKPOINT_STORAGE, "filesystem");
		config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config1.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config1.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);
		config1.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		final Configuration config2 = new Configuration();
		config2.set(CheckpointingOptions.CHECKPOINT_STORAGE, FileSystemCheckpointStorageFactory.class.getName());
		config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir);
		config2.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config2.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, threshold);
		config1.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, minWriteBufferSize);
		config2.setBoolean(CheckpointingOptions.ASYNC_SNAPSHOTS, async);

		CheckpointStorage storage1 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config1, cl, null);
		CheckpointStorage storage2 = CheckpointStorageLoader.loadCheckpointStorageFromConfig(config2, cl, null);

		Assert.assertThat(storage1, Matchers.instanceOf(FileSystemCheckpointStorage.class));
		Assert.assertThat(storage2, Matchers.instanceOf(FileSystemCheckpointStorage.class));

		FileSystemCheckpointStorage fs1 = (FileSystemCheckpointStorage) storage1;
		FileSystemCheckpointStorage fs2 = (FileSystemCheckpointStorage) storage1;

		assertEquals(expectedCheckpointsPath, fs1.getCheckpointPath());
		assertEquals(expectedCheckpointsPath, fs2.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs1.getSavepointPath());
		assertEquals(expectedSavepointsPath, fs2.getSavepointPath());
		assertEquals(threshold.getBytes(), fs1.getMinFileSizeThreshold());
		assertEquals(threshold.getBytes(), fs2.getMinFileSizeThreshold());
		assertEquals(Math.max(threshold.getBytes(), minWriteBufferSize), fs1.getWriteBufferSize());
		assertEquals(Math.max(threshold.getBytes(), minWriteBufferSize), fs2.getWriteBufferSize());
	}

	/**
	 * Validates taking the application-defined file system state backend and adding with additional
	 * parameters from the cluster configuration, but giving precedence to application-defined
	 * parameters over configuration-defined parameters.
	 */
	@Test
	public void testLoadFileSystemCheckpointStorageMixed() throws Exception {
		final Path appCheckpointDir = new Path(tmp.newFolder().toURI());
		final String checkpointDir = new Path(tmp.newFolder().toURI()).toString();
		final String savepointDir = new Path(tmp.newFolder().toURI()).toString();

		final Path expectedSavepointsPath = new Path(savepointDir);

		final int threshold = 1000000;
		final int writeBufferSize = 4000000;

		final FileSystemCheckpointStorage storage = new FileSystemCheckpointStorage(appCheckpointDir, threshold, writeBufferSize);

		final Configuration config = new Configuration();
		config.set(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager"); // this should not be picked up
		config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir); // this should not be picked up
		config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir);
		config.set(CheckpointingOptions.FS_SMALL_FILE_THRESHOLD, MemorySize.parse("20")); // this should not be picked up
		config.setInteger(CheckpointingOptions.FS_WRITE_BUFFER_SIZE, 3000000); // this should not be picked up

		final CheckpointStorage loadedStorage =
				CheckpointStorageLoader.fromApplicationOrConfigOrDefault(storage,
						null,
						new ModernStateBackend(), config, cl, null);
		Assert.assertThat(loadedStorage, Matchers.instanceOf(FileSystemCheckpointStorage.class));

		final FileSystemCheckpointStorage fs = (FileSystemCheckpointStorage) loadedStorage;
		assertEquals(appCheckpointDir, fs.getCheckpointPath());
		assertEquals(expectedSavepointsPath, fs.getSavepointPath());
		assertEquals(threshold, fs.getMinFileSizeThreshold());
		assertEquals(writeBufferSize, fs.getWriteBufferSize());
	}

	// ------------------------------------------------------------------------
	//  High-availability default
	// ------------------------------------------------------------------------

	/**
	 * This tests the default behaviour in the case of configured high-availability.
	 * Specially, if not configured checkpoint directory, the memory state backend
	 * would not create arbitrary directory under HA persistence directory.
	 */
	@Test
	public void testHighAvailabilityDefault() throws Exception {
		final String haPersistenceDir = new Path(tmp.newFolder().toURI()).toString();
		testMemoryBackendHighAvailabilityDefault(haPersistenceDir, null);

		final Path checkpointPath = new Path(tmp.newFolder().toURI().toString());
		testMemoryBackendHighAvailabilityDefault(haPersistenceDir, checkpointPath);
	}

	@Test
	public void testHighAvailabilityDefaultLocalPaths() throws Exception {
		final String haPersistenceDir = new Path(tmp.newFolder().getAbsolutePath()).toString();
		testMemoryBackendHighAvailabilityDefault(haPersistenceDir, null);

		final Path checkpointPath = new Path(tmp.newFolder().toURI().toString()).makeQualified(
				FileSystem.getLocalFileSystem());
		testMemoryBackendHighAvailabilityDefault(haPersistenceDir, checkpointPath);
	}

	private void testMemoryBackendHighAvailabilityDefault(String haPersistenceDir, Path checkpointPath) throws Exception {
		final Configuration config1 = new Configuration();
		config1.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config1.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config1.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		final Configuration config2 = new Configuration();
		config2.setString(CheckpointingOptions.CHECKPOINT_STORAGE, "jobmanager");
		config2.setString(HighAvailabilityOptions.HA_MODE, "zookeeper");
		config2.setString(HighAvailabilityOptions.HA_CLUSTER_ID, "myCluster");
		config2.setString(HighAvailabilityOptions.HA_STORAGE_PATH, haPersistenceDir);

		if (checkpointPath != null) {
			config1.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
			config2.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointPath.toUri().toString());
		}

		final JobManagerCheckpointStorage storage = new JobManagerCheckpointStorage();

		final CheckpointStorage loaded1 = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(storage, null, new ModernStateBackend(), config1, cl, null);
		final CheckpointStorage loaded2 = CheckpointStorageLoader.fromApplicationOrConfigOrDefault(null, null, new ModernStateBackend(), config2, cl, null);

		Assert.assertThat(loaded1, Matchers.instanceOf(JobManagerCheckpointStorage.class));
		Assert.assertThat(loaded2, Matchers.instanceOf(JobManagerCheckpointStorage.class));

		final JobManagerCheckpointStorage memStorage1 = (JobManagerCheckpointStorage) loaded1;
		final JobManagerCheckpointStorage memStorage2 = (JobManagerCheckpointStorage) loaded2;

		assertNull(memStorage1.getSavepointPath());
		assertNull(memStorage2.getSavepointPath());

		if (checkpointPath != null) {
			assertNotNull(memStorage1.getCheckpointPath());
			assertNotNull(memStorage2.getCheckpointPath());

			assertEquals(checkpointPath, memStorage1.getCheckpointPath());
			assertEquals(checkpointPath, memStorage2.getCheckpointPath());
		} else {
			assertNull(memStorage1.getCheckpointPath());
			assertNull(memStorage2.getCheckpointPath());
		}
	}


	// A state backend that also implements checkpoint storage.
	static final class LegacyStateBackend implements StateBackend, CheckpointStorage {
		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
			return null;
		}

		@Override
		public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
			return null;
		}

		@Override
		public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
				Environment env,
				JobID jobID,
				String operatorIdentifier,
				TypeSerializer<K> keySerializer,
				int numberOfKeyGroups,
				KeyGroupRange keyGroupRange,
				TaskKvStateRegistry kvStateRegistry,
				TtlTimeProvider ttlTimeProvider,
				MetricGroup metricGroup,
				Collection<KeyedStateHandle> stateHandles,
				CloseableRegistry cancelStreamRegistry) throws Exception {
			return null;
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
				Environment env,
				String operatorIdentifier,
				Collection<OperatorStateHandle> stateHandles,
				CloseableRegistry cancelStreamRegistry) throws Exception {
			return null;
		}
	}

	static final class ModernStateBackend implements StateBackend {

		@Override
		public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
				Environment env,
				JobID jobID,
				String operatorIdentifier,
				TypeSerializer<K> keySerializer,
				int numberOfKeyGroups,
				KeyGroupRange keyGroupRange,
				TaskKvStateRegistry kvStateRegistry,
				TtlTimeProvider ttlTimeProvider,
				MetricGroup metricGroup,
				Collection<KeyedStateHandle> stateHandles,
				CloseableRegistry cancelStreamRegistry) throws Exception {
			return null;
		}

		@Override
		public OperatorStateBackend createOperatorStateBackend(
				Environment env,
				String operatorIdentifier,
				Collection<OperatorStateHandle> stateHandles,
				CloseableRegistry cancelStreamRegistry) throws Exception {
			return null;
		}
	}

	static final class MockStorage implements CheckpointStorage {

		@Override
		public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer) throws IOException {
			return null;
		}

		@Override
		public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
			return null;
		}
	}

	static final class WorkingFactory implements CheckpointStorageFactory<MockStorage> {

		@Override
		public MockStorage createFromConfig(
				ReadableConfig config,
				ClassLoader classLoader) throws IllegalConfigurationException, IOException {
			return new MockStorage();
		}
	}

	static final class FailingFactory implements CheckpointStorageFactory<CheckpointStorage> {

		@Override
		public CheckpointStorage createFromConfig(
				ReadableConfig config,
				ClassLoader classLoader) throws IllegalConfigurationException, IOException {
			throw new IOException("fail!");
		}
	}
}
