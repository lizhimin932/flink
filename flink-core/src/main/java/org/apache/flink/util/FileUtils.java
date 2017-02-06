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

package org.apache.flink.util;

import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This is a utility class to deal files and directories. Contains utilities for recursive
 * deletion and creation of temporary files.
 */
public final class FileUtils {

	/** The alphabet to construct the random part of the filename from. */
	private static final char[] ALPHABET = 
			{ '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'a', 'b', 'c', 'd', 'e', 'f' };

	/** The length of the random part of the filename. */
	private static final int RANDOM_FILE_NAME_LENGTH = 12;

	// ------------------------------------------------------------------------

	/**
	 * Constructs a random filename with the given prefix and
	 * a random part generated from hex characters.
	 * 
	 * @param prefix
	 *        the prefix to the filename to be constructed
	 * @return the generated random filename with the given prefix
	 */
	public static String getRandomFilename(final String prefix) {
		final StringBuilder stringBuilder = new StringBuilder(prefix);

		for (int i = 0; i < RANDOM_FILE_NAME_LENGTH; i++) {
			stringBuilder.append(ALPHABET[(int) Math.floor(Math.random() * (double) ALPHABET.length)]);
		}

		return stringBuilder.toString();
	}
	
	// ------------------------------------------------------------------------
	//  Simple reading and writing of files
	// ------------------------------------------------------------------------
	
	public static String readFile(File file, String charsetName) throws IOException {
		byte[] bytes = Files.readAllBytes(file.toPath());
		return new String(bytes, charsetName);
	}

	public static String readFileUtf8(File file) throws IOException {
		return readFile(file, "UTF-8");
	}

	public static void writeFile(File file, String contents, String encoding) throws IOException {
		byte[] bytes = contents.getBytes(encoding);
		Files.write(file.toPath(), bytes, StandardOpenOption.WRITE);
	}
	
	public static void writeFileUtf8(File file, String contents) throws IOException {
		writeFile(file, contents, "UTF-8");
	}

	// ------------------------------------------------------------------------
	//  Deleting directories on standard File Systems
	// ------------------------------------------------------------------------

	/**
	 * Removes the given file or directory recursively.
	 * 
	 * <p>If the file or directory does not exist, this does not throw an exception, but simply does nothing.
	 * It considers the fact that a file-to-be-deleted is not present a success.
	 * 
	 * <p>This method is safe against other concurrent deletion attempts.
	 * 
	 * @param file The file or directory to delete.
	 * 
	 * @throws IOException Thrown if the directory could not be cleaned for some reason, for example
	 *                     due to missing access/write permissions.
	 */
	public static void deleteFileOrDirectory(File file) throws IOException {
		checkNotNull(file, "file");

		if (file.isDirectory()) {
			// file exists and is directory
			deleteDirectory(file);
		}
		else if (file.exists()) {
			try {
				Files.delete(file.toPath());
				System.out.println("Fil = " + file.toString() + " - " + Thread.currentThread().getName() + " - Del - " + file.exists());
			}
			catch (NoSuchFileException e) {
				// if the file is already gone (concurrently), we don't mind
			} catch (AccessDeniedException e) {
				// this may occur on Windows if another process is currently
				// deleting the file. We double check here to make sure the
				// file was actually deleted by another process. Note that this
				// isn't a perfect solution, but it's better than nothing.
				if (Files.exists(file.toPath())) {
					throw e;
				}
			}
		}
		// else: already deleted
	}

	/**
	 * Deletes the given directory recursively.
	 * 
	 * <p>If the directory does not exist, this does not throw an exception, but simply does nothing.
	 * It considers the fact that a directory-to-be-deleted is not present a success.
	 * 
	 * <p>This method is safe against other concurrent deletion attempts.
	 * 
	 * @param directory The directory to be deleted.
	 * @throws IOException Thrown if the given file is not a directory, or if the directory could not be
	 *                     deleted for some reason, for example due to missing access/write permissions.
	 */
	public static void deleteDirectory(File directory) throws IOException {
		checkNotNull(directory, "directory");

		if (directory.isDirectory()) {
			// directory exists and is a directory

			// empty the directory first
			try {
				cleanDirectory(directory);
			}
			catch (FileNotFoundException ignored) {
				// someone concurrently deleted the directory, nothing to do for us
				return;
			}

			java.nio.file.Path directoryPath = directory.toPath();
			if (OperatingSystem.isWindows()) {
				// delete the directory. this fails if the directory is not empty, meaning
				// if new files got concurrently created. we want to fail then.
				try {
					Files.delete(directoryPath);
					System.out.println("Dir = " + directory.toString() + " - " + Thread.currentThread().getName() + " - Del - " + directory.exists());
				} catch (NoSuchFileException ignored) {
					// if someone else deleted this concurrently, we don't mind
					// the result is the same for us, after all
				} catch (AccessDeniedException e) {
					// This may occur on Windows if another process is currently
					// deleting the file, since the file must be opened in order
					// to delete it. We double check here to make sure the file
					// was actually deleted by another process. Note that this
					// isn't a perfect solution, but it's better than nothing.
					if (Files.exists(directoryPath)) {
						throw e;
					}
				} catch (DirectoryNotEmptyException e) {
					// This may occur on Windows for some reason even for empty
					// directories. Apparently there's a timing/visibility
					// issue when concurrently deleting the contents of a directory 
					// and afterwards deleting the directory itself.
					
					File[] contents = directory.listFiles();
					System.out.println("Dir = " + directory.toString() + " - " + Thread.currentThread().getName() + " - Fail - " + Arrays.toString(contents));
					if (contents != null && contents.length > 0) {
						boolean empty = true;
						for (File f : contents) {
							if (f.exists()) {
								empty = false;
							}
							System.out.println(f + " - " + empty);
						}
						if (!empty) {
							System.out.flush();
							throw new RuntimeException("Dir = " + directory.toString() + " - " + Thread.currentThread().getName(), e);
						}
					}
				}
			} else {
				// delete the directory. this fails if the directory is not empty, meaning
				// if new files got concurrently created. we want to fail then.
				try {
					Files.delete(directoryPath);
				}
				catch (NoSuchFileException ignored) {
					// if someone else deleted this concurrently, we don't mind
					// the result is the same for us, after all
				}
			}
		}
		else if (directory.exists()) {
			// exists but is file, not directory
			// either an error from the caller, or concurrently a file got created
			throw new IOException(directory + " is not a directory");
		}
		// else: does not exist, which is okay (as if deleted)
	}

	/**
	 * Deletes the given directory recursively, not reporting any I/O exceptions
	 * that occur.
	 * 
	 * <p>This method is identical to {@link FileUtils#deleteDirectory(File)}, except that it
	 * swallows all exceptions and may leave the job quietly incomplete.
	 * 
	 * @param directory The directory to delete.
	 */
	public static void deleteDirectoryQuietly(File directory) {
		if (directory == null) {
			return;
		}

		// delete and do not report if it fails
		try {
			deleteDirectory(directory);
		} catch (Exception ignored) {}
	}

	/**
	 * Removes all files contained within a directory, without removing the directory itself. 
	 * 
	 * <p>This method is safe against other concurrent deletion attempts.
	 * 
	 * @param directory The directory to remove all files from.
	 * 
	 * @throws FileNotFoundException Thrown if the directory itself does not exist.
	 * @throws IOException Thrown if the file indicates a proper file and not a directory, or if
	 *                     the directory could not be cleaned for some reason, for example
	 *                     due to missing access/write permissions.
	 */
	public static void cleanDirectory(File directory) throws IOException, FileNotFoundException {
		checkNotNull(directory, "directory");

		if (directory.isDirectory()) {
			final File[] files = directory.listFiles();
			System.out.println("Dir = " + directory.toString() + " - " + Thread.currentThread().getName() + " - Attm - " + Arrays.toString(files));

			if (files == null) {
				// directory does not exist any more or no permissions
				if (directory.exists()) {
					throw new IOException("Failed to list contents of " + directory);
				} else {
					throw new FileNotFoundException(directory.toString());
				}
			}

			// remove all files in the directory
			for (File file : files) {
				if (file != null) {
					deleteFileOrDirectory(file);
				}
			}
		}
		else if (directory.exists()) {
			throw new IOException(directory + " is not a directory but a regular file");
		}
		else {
			// else does not exist at all
			throw new FileNotFoundException(directory.toString());
		}
	}

	// ------------------------------------------------------------------------
	//  Deleting directories on Flink FileSystem abstraction
	// ------------------------------------------------------------------------

	/**
	 * Deletes the path if it is empty. A path can only be empty if it is a directory which does
	 * not contain any other directories/files.
	 *
	 * @param fileSystem to use
	 * @param path to be deleted if empty
	 * @return true if the path could be deleted; otherwise false
	 * @throws IOException if the delete operation fails
	 */
	public static boolean deletePathIfEmpty(FileSystem fileSystem, Path path) throws IOException {
		FileStatus[] fileStatuses = null;

		try {
			fileStatuses = fileSystem.listStatus(path);
		} catch (Exception ignored) {}

		// if there are no more files or if we couldn't list the file status try to delete the path
		if (fileStatuses == null || fileStatuses.length == 0) {
			// attempt to delete the path (will fail and be ignored if the path now contains
			// some files (possibly added concurrently))
			return fileSystem.delete(path, false);
		} else {
			return false;
		}
	}
	
	// ------------------------------------------------------------------------

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private FileUtils() {}
}
