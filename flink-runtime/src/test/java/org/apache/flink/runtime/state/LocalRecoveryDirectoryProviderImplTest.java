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
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Tests for {@link LocalRecoveryDirectoryProvider}.
 */
public class LocalRecoveryDirectoryProviderImplTest {

	private static final JobID JOB_ID = new JobID();
	private static final AllocationID ALLOCATION_ID = new AllocationID();
	private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
	private static final int SUBTASK_INDEX = 0;

	private TemporaryFolder tmpFolder;
	private LocalRecoveryDirectoryProviderImpl directoryProvider;
	private File[] rootFolders;

	@Before
	public void setup() throws IOException {
		this.tmpFolder = new TemporaryFolder();
		this.tmpFolder.create();
		this.rootFolders = new File[]{tmpFolder.newFolder(), tmpFolder.newFolder(), tmpFolder.newFolder()};
		this.directoryProvider = new LocalRecoveryDirectoryProviderImpl(
			rootFolders,
			JOB_ID,
			ALLOCATION_ID,
			JOB_VERTEX_ID,
			SUBTASK_INDEX);
	}

	@After
	public void tearDown() {
		this.tmpFolder.delete();
	}

	@Test
	public void rootDirectory() throws Exception {
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(rootFolders[i % rootFolders.length], directoryProvider.rootDirectory(i));
		}
	}

	@Test
	public void selectRootDirectory() throws Exception {
		for (int i = 0; i < rootFolders.length; ++i) {
			Assert.assertEquals(rootFolders[i], directoryProvider.selectRootDirectory(i));
		}
	}

	@Test
	public void rootDirectoryCount() throws Exception {
		Assert.assertEquals(rootFolders.length, directoryProvider.rootDirectoryCount());
	}

	@Test
	public void jobAndAllocationBaseDir() {
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(
				new File(directoryProvider.rootDirectory(i), directoryProvider.allocationSubDirString()),
				directoryProvider.allocationBaseDirectory(i));
		}
	}

	@Test
	public void checkpointBaseDir() {
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(
				new File(
					directoryProvider.allocationBaseDirectory(i),
					directoryProvider.jobCheckpointSubDirString(i)),
				directoryProvider.jobAndCheckpointBaseDirectory(i));
		}
	}

	@Test
	public void subtaskSpecificDirectory() {
		for (int i = 0; i < 10; ++i) {
			Assert.assertEquals(
				new File(
					directoryProvider.jobAndCheckpointBaseDirectory(i),
					directoryProvider.subtaskSubDirString()),
				directoryProvider.subtaskSpecificCheckpointDirectory(i));
		}
	}

	@Test
	public void testPathStringConstants() {
		Assert.assertEquals(
			directoryProvider.allocationSubDirString(),
			"aid_" + ALLOCATION_ID);

		Assert.assertEquals(
			directoryProvider.jobCheckpointSubDirString(42),
			"jid_" + JOB_ID + File.separator + "chk_" + 42);

		Assert.assertEquals(
			directoryProvider.subtaskSubDirString(),
			"vtx_" + JOB_VERTEX_ID + Path.SEPARATOR + SUBTASK_INDEX);
	}

	@Test
	public void testPreconditionsNotNullFiles() {
		try {
			new LocalRecoveryDirectoryProviderImpl(new File[]{null}, JOB_ID, ALLOCATION_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
			Assert.fail();
		} catch (NullPointerException ignore) {
		}
	}

	@Test
	public void testPreconditionsNonExistingFolder() {
		try {
			new LocalRecoveryDirectoryProviderImpl(new File[]{new File("123")}, JOB_ID, ALLOCATION_ID, JOB_VERTEX_ID, SUBTASK_INDEX);
			Assert.fail();
		} catch (IllegalStateException ignore) {
		}
	}

	/**
	 * This tests that the proactive cleanup will remove all directories under an allocation base dir that are not from
	 * the current job.
	 */
	@Test
	public void testProactiveCleanup() throws IOException {

		List<File> toCleanup = new ArrayList<>(2 * directoryProvider.rootDirectoryCount());
		List<File> toPreserve = new ArrayList<>(directoryProvider.rootDirectoryCount());

		for (int i = 0; i< directoryProvider.rootDirectoryCount(); ++i) {

			File allocBaseDir = directoryProvider.selectAllocationBaseDirectory(i);

			// This should NOT be cleaned up later.
			File jobChkDir = new File(allocBaseDir, directoryProvider.jobSubDirString());
			Assert.assertTrue(jobChkDir.mkdirs());

			// This should be cleaned up later.
			File oldDir = new File(allocBaseDir, String.valueOf(UUID.randomUUID()));
			Assert.assertTrue(oldDir.mkdirs());

			// This should be cleaned up later.
			File oldFile = new File(oldDir, String.valueOf(UUID.randomUUID()));
			Assert.assertTrue(oldFile.createNewFile());

			toPreserve.add(jobChkDir);
			toCleanup.add(oldDir);
			toCleanup.add(oldFile);
		}

		for (File file : toCleanup) {
			Assert.assertTrue(file.exists());
		}

		for (File file : toPreserve) {
			Assert.assertTrue(file.exists());
		}

		directoryProvider.cleanupAllocationBaseDirectories();

		for (File file : toCleanup) {
			Assert.assertFalse(file.exists());
		}

		for (File file : toPreserve) {
			Assert.assertTrue(file.exists());
		}
	}
}
