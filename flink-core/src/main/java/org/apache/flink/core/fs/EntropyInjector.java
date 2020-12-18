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

package org.apache.flink.core.fs;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.FileSystem.WriteMode;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * This class offers utilities for entropy injection for FileSystems that implement
 * {@link EntropyInjectingFileSystem}.
 */
@PublicEvolving
public class EntropyInjector {

	/**
	 * Handles entropy injection across regular and entropy-aware file systems.
	 *
	 * <p>If the given file system is entropy-aware (a implements {@link EntropyInjectingFileSystem}),
	 * then this method replaces the entropy marker in the path with random characters.
	 * The entropy marker is defined by {@link EntropyInjectingFileSystem#getEntropyInjectionKey()}.
	 *
	 * <p>If the given file system does not implement {@code EntropyInjectingFileSystem},
	 * then this method delegates to {@link FileSystem#create(Path, WriteMode)} and
	 * returns the same path in the resulting {@code OutputStreamAndPath}.
	 */
	public static OutputStreamAndPath createEntropyAware(
			FileSystem fs,
			Path path,
			WriteMode writeMode) throws IOException {

		// check and possibly inject entropy into the path
		final EntropyInjectingFileSystem efs = getEntropyFs(fs);
		final Path processedPath = efs == null ? path : resolveEntropy(path, efs, efs.generateEntropy());

		// create the stream on the original file system to let the safety net
		// take its effect
		final FSDataOutputStream out = fs.create(processedPath, writeMode);
		return new OutputStreamAndPath(out, processedPath);
	}

	/**
	 * Replaces the entropy marker string in the path with the configured entropy key replacement, if the given
	 * file system is an entropy-injecting file system (implements {@link EntropyInjectingFileSystem}) and
	 * the entropy marker key is present. Otherwise, this returns the path as is.
	 *
	 * @param path The path to filter.
	 * @return The path without the marker string.
	 */
	public static Path replaceEntropyMarkerIfPresent(FileSystem fs, Path path) {
		final EntropyInjectingFileSystem efs = getEntropyFs(fs);
		if (efs == null) {
			return path;
		}
		else  {
			try {
				String entropyKeyReplacement = efs.getEntropyKeyReplacement();
				return resolveEntropy(path, efs, entropyKeyReplacement);
			}
			catch (IOException e) {
				// this should never happen, because the path was valid before and we only remove characters.
				// rethrow to silence the compiler
				throw new FlinkRuntimeException(e.getMessage(), e);
			}
		}
	}

	// ------------------------------------------------------------------------

	public static boolean isEntropyInjecting(FileSystem fs) {
		return getEntropyFs(fs) != null;
	}

	@Nullable
	private static EntropyInjectingFileSystem getEntropyFs(FileSystem fs) {
		if (fs instanceof EntropyInjectingFileSystem) {
			return (EntropyInjectingFileSystem) fs;
		}
		else if (fs instanceof SafetyNetWrapperFileSystem) {
			FileSystem delegate = ((SafetyNetWrapperFileSystem) fs).getWrappedDelegate();
			if (delegate instanceof EntropyInjectingFileSystem) {
				return (EntropyInjectingFileSystem) delegate;
			}
			else if (delegate instanceof PluginFileSystemFactory.ClassLoaderFixingFileSystem) {
				FileSystem innerFs = ((PluginFileSystemFactory.ClassLoaderFixingFileSystem) delegate).getInner();
				if (innerFs instanceof EntropyInjectingFileSystem) {
					return (EntropyInjectingFileSystem) innerFs;
				}
				return null;
			}
			else {
				return null;
			}
		}
		else {
			return null;
		}
	}

	@VisibleForTesting
	static Path resolveEntropy(Path path, EntropyInjectingFileSystem efs, String entropyValue) throws IOException {
		final String entropyInjectionKey = efs.getEntropyInjectionKey();

		if (entropyInjectionKey == null) {
			return path;
		}
		else {
			final URI originalUri = path.toUri();
			final String checkpointPath = originalUri.getPath();

			final int indexOfKey = checkpointPath.indexOf(entropyInjectionKey);
			if (indexOfKey == -1) {
				return path;
			}
			else {
				try {
					return new Path(new URI(
							originalUri.getScheme(),
							originalUri.getAuthority(),
							rewriteCheckpointPath(checkpointPath, entropyInjectionKey, entropyValue),
							originalUri.getQuery(),
							originalUri.getFragment()).normalize());
				}
				catch (URISyntaxException e) {
					// this could only happen if the injected entropy string contains invalid characters
					throw new IOException("URI format error while processing path for entropy injection", e);
				}
			}
		}
	}

	static String rewriteCheckpointPath(String checkpointPath, String entropyKey, String entropyValue) {
		final int indexOfKey = checkpointPath.indexOf(entropyKey);
		if (indexOfKey == -1) {
			return checkpointPath;
		}
		else {
			final StringBuilder buffer = new StringBuilder(checkpointPath.length());
			buffer.append(checkpointPath, 0, indexOfKey);
			buffer.append(entropyValue);
			buffer.append(checkpointPath, indexOfKey + entropyKey.length(), checkpointPath.length());
			return buffer.toString();
		}
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated. */
	private EntropyInjector() {}
}
