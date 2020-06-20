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

package org.apache.flink.tests.util.flink;

import org.apache.flink.tests.util.parameters.ParameterProperty;
import org.apache.flink.util.FileUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Optional;

/**
 * A {@link FlinkResourceFactory} for the {@link LocalStandaloneFlinkResource}.
 */
public final class LocalStandaloneFlinkResourceFactory implements FlinkResourceFactory {
	private static final Logger LOG = LoggerFactory.getLogger(LocalStandaloneFlinkResourceFactory.class);

	private static final ParameterProperty<Path> PROJECT_ROOT_DIRECTORY = new ParameterProperty<>("rootDir", Paths::get);
	private static final ParameterProperty<Path> DISTRIBUTION_DIRECTORY = new ParameterProperty<>("distDir", Paths::get);
	private static final ParameterProperty<Path> DISTRIBUTION_LOG_BACKUP_DIRECTORY = new ParameterProperty<>("logBackupDir", Paths::get);

	@Override
	public FlinkResource create(FlinkResourceSetup setup) {
		Optional<Path> distributionDirectory = DISTRIBUTION_DIRECTORY.get();
		if (!distributionDirectory.isPresent()) {
			LOG.debug("The '{}' property was not set; attempting to automatically determine distribution location.", DISTRIBUTION_DIRECTORY.getPropertyName());

			Path projectRootPath;
			Optional<Path> projectRoot = PROJECT_ROOT_DIRECTORY.get();
			if (projectRoot.isPresent()) {
				// running with maven
				projectRootPath = projectRoot.get();
			} else {
				// running in the IDE; working directory is test module
				Optional<Path> projectRootDirectory = findProjectRootDirectory(Paths.get("").toAbsolutePath());
				// this distinction is required in case this class is used outside of Flink
				if (projectRootDirectory.isPresent()) {
					projectRootPath = projectRootDirectory.get();
				} else {
					throw new IllegalArgumentException(
						"The 'distDir' property was not set and the flink-dist module could not be found automatically." +
							" Please point the 'distDir' property to the directory containing distribution; you can set it when running maven via -DdistDir=<path> .");
				}
			}
			Optional<Path> distribution = findDistribution(projectRootPath);
			if (!distribution.isPresent()) {
				throw new IllegalArgumentException(
					"The 'distDir' property was not set and a distribution could not be found automatically." +
						" Please point the 'distDir' property to the directory containing distribution; you can set it when running maven via -DdistDir=<path> .");
			} else {
				distributionDirectory = distribution;
			}
		}
		Optional<Path> logBackupDirectory = DISTRIBUTION_LOG_BACKUP_DIRECTORY.get();
		if (!logBackupDirectory.isPresent()) {
			LOG.warn("Property {} not set, logs will not be backed up in case of test failures.", DISTRIBUTION_LOG_BACKUP_DIRECTORY.getPropertyName());
		}
		return new LocalStandaloneFlinkResource(distributionDirectory.get(), logBackupDirectory.orElse(null), setup);
	}

	private static Optional<Path> findProjectRootDirectory(Path currentDirectory) {
		// move up the module structure until we find flink-dist; relies on all modules being prefixed with 'flink'
		do {
			if (Files.exists(currentDirectory.resolve("flink-dist"))) {
				return Optional.of(currentDirectory);
			}
			currentDirectory = currentDirectory.getParent();
		}  while (currentDirectory.getFileName().toString().startsWith("flink"));
		return Optional.empty();
	}

	private static Optional<Path> findDistribution(Path projectRootDirectory) {
		final Path distTargetDirectory = projectRootDirectory.resolve("flink-dist").resolve("target");
		try {
			Collection<Path> paths = FileUtils.listFilesInDirectory(
				distTargetDirectory,
				LocalStandaloneFlinkResourceFactory::isDistribution);
			if (paths.size() == 0) {
				// likely due to flink-dist not having been built
				return Optional.empty();
			}
			if (paths.size() > 1) {
				// target directory can contain distributions for multiple versions, or it's just a dirty environment
				LOG.warn("Detected multiple distributions under flink-dist/target. It is recommended to explicitly" +
					" select the distribution by setting the '{}}' property.", DISTRIBUTION_DIRECTORY.getPropertyName());
			}
			// jar should be in /lib; first getParent() returns /lib, second getParent() returns distribution directory
			return Optional.of(paths.iterator().next().getParent().getParent());
		} catch (IOException e) {
			LOG.error("Error while searching for distribution.", e);
			return Optional.empty();
		}
	}

	private static boolean isDistribution(Path path) {
		// check for `lib/flink-dist*'
		// searching for the flink-dist jar is not sufficient since it also exists in the modules 'target' directory
		return path.getFileName().toString().contains("flink-dist")
			&& path.getParent().getFileName().toString().equals("lib");
	}
}

