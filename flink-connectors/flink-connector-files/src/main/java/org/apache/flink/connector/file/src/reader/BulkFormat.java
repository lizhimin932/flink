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

package org.apache.flink.connector.file.src.reader;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.util.MutableRecordAndPosition;
import org.apache.flink.connector.file.src.util.RecordAndPosition;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;

/**
 * The {@code BulkFormat} reads and decodes batches of records at a time. Examples of bulk formats
 * are formats like ORC or Parquet.
 *
 * <p>The actual reading is done by the {@link BulkFormat.Reader}, which is created in the
 * {@link BulkFormat#createReader(Configuration, Path, long, long)} or
 * {@link BulkFormat#createReader(Configuration, Path, long, long, long)} methods.
 * The outer class acts mainly as a configuration holder and factory for the reader.
 *
 * <h2>Checkpointing</h2>
 *
 * <p>The bulk reader returns an iterator structure per batch. The iterator produces records together
 * with a position. That position is the point from where the reading can be resumed AFTER
 * the records was emitted. So that position points effectively to the record AFTER the current record.
 *
 * <p>The simplest way to return this position information is to always assume a zero offset in the file
 * and simply increment the record count. Note that in this case the fist record would be returned with
 * a record count of one, the second one with a record count of two, etc.
 *
 * <p>Formats that have the ability to efficiently seek to a record (or to every n-th record) by offset
 * in the file can work with the position field to avoid having to read and discard many records on recovery.
 *
 * <h2>Serializable</h2>
 *
 * <p>Like many other API classes in Flink, the outer class is serializable to support sending instances
 * to distributed workers for parallel execution. This is purely short-term serialization for RPC and
 * no instance of this will be long-term persisted in a serialized form.
 *
 * <h2>Record Batching</h2>
 *
 * <p>Internally in the file source, the readers pass batches of records from the reading
 * threads (that perform the typically blocking I/O operations) to the async mailbox threads that
 * do the streaming and batch data processing. Passing records in batches (rather than one-at-a-time)
 * much reduce the thread-to-thread handover overhead.
 *
 * <p>For the {@code BulkFormat}, one batch (as returned by {@link BulkFormat.Reader#readBatch()}) is
 * handed over as one.
 */
public interface BulkFormat<T> extends Serializable {

	/**
	 * Creates a new reader that reads from {@code filePath} starting at {@code offset} and reads
	 * until {@code length} bytes after the offset.
	 */
	default BulkFormat.Reader<T> createReader(Configuration config, Path filePath, long offset, long length) throws IOException {
		return createReader(config, filePath, offset, length, 0L);
	}

	/**
	 * Creates a new reader that reads from {@code filePath} starting at {@code offset} and reads
	 * until {@code length} bytes after the offset. A number of {@code recordsToSkip} records should be
	 * read and discarded after the offset. This is typically part of restoring a reader to a checkpointed
	 * position.
	 */
	BulkFormat.Reader<T> createReader(Configuration config, Path filePath, long offset, long length, long recordsToSkip) throws IOException;

	/**
	 * Gets the type produced by this format. This type will be the type produced by the file
	 * source as a whole.
	 */
	TypeInformation<T> getProducedType();


	// ------------------------------------------------------------------------

	/**
	 * The actual reader that reads the batches of records.
	 */
	interface Reader<T> extends Closeable {

		/**
		 * Reads one batch. The method should return null when reaching the end of the input.
		 * The returned batch will be handed over to the processing threads as one.
		 *
		 * <p>The returned iterator object and any contained objects may be held onto by the file source
		 * for some time, so it should not be immediately reused by the reader.
		 *
		 * <p>To implement reuse and to save object allocation, consider using a
		 * {@link org.apache.flink.connector.file.src.util.Pool} and recycle objects into the Pool in the
		 * the {@link RecordIterator#close()} method.
		 */
		@Nullable
		RecordIterator<T> readBatch() throws IOException;

		/**
		 * Closes the reader and should release all resources.
		 */
		@Override
		void close() throws IOException;
	}

	// ------------------------------------------------------------------------

	/**
	 * An iterator over records with their position in the file.
	 * The iterator is closeable to support clean resource release and recycling.
	 *
	 * @param <T> The type of the record.
	 */
	interface RecordIterator<T> {

		/**
		 * Gets the next record from the file, together with its position.
		 *
		 * <p>The position information returned with the record point to the record AFTER the returned
		 * record, because it defines the point where the reading should resume once the current record
		 * is emitted. The position information is put in the source's state when the record
		 * is emitted. If a checkpoint is taken directly after the record is emitted, the checkpoint must
		 * to describe where to resume the source reading from after that record.
		 *
		 * <p>Objects returned by this method may be reused by the iterator. By the time that this
		 * method is called again, no object returned from the previous call will be referenced any more.
		 * That makes it possible to have a single {@link MutableRecordAndPosition} object and return the
		 * same instance (with updated record and position) on every call.
		 */
		@Nullable
		RecordAndPosition<T> next();

		/**
		 * Closes this iterator. This is not supposed to close the reader and its resources, but
		 * is simply a signal that this iterator is no used any more. This method can be used as a
		 * hook to recycle/reuse heavyweight object structures.
		 */
		void close();
	}
}
