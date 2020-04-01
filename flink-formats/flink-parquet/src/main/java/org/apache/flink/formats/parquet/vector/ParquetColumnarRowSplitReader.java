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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.formats.parquet.vector.reader.AbstractColumnReader;
import org.apache.flink.formats.parquet.vector.reader.ColumnReader;
import org.apache.flink.table.dataformat.ColumnarRow;
import org.apache.flink.table.dataformat.vector.ColumnVector;
import org.apache.flink.table.dataformat.vector.VectorizedColumnBatch;
import org.apache.flink.table.dataformat.vector.writable.WritableColumnVector;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createColumnReader;
import static org.apache.flink.formats.parquet.vector.ParquetSplitReaderUtil.createWritableColumnVector;
import static org.apache.parquet.filter2.compat.RowGroupFilter.filterRowGroups;
import static org.apache.parquet.format.converter.ParquetMetadataConverter.range;
import static org.apache.parquet.hadoop.ParquetFileReader.readFooter;
import static org.apache.parquet.hadoop.ParquetInputFormat.getFilter;

/**
 * This reader is used to read a {@link VectorizedColumnBatch} from input split.
 */
public class ParquetColumnarRowSplitReader implements Closeable {

	private final boolean utcTimestamp;

	private final MessageType fileSchema;

	private final MessageType requestedSchema;

	/**
	 * The total number of rows this RecordReader will eventually read. The sum of the rows of all
	 * the row groups.
	 */
	private final long totalRowCount;

	private final WritableColumnVector[] writableVectors;

	private final VectorizedColumnBatch columnarBatch;

	private final ColumnarRow row;

	private final LogicalType[] selectedTypes;

	private final int batchSize;

	private ParquetFileReader reader;

	/**
	 * For each request column, the reader to read this column. This is NULL if this column is
	 * missing from the file, in which case we populate the attribute with NULL.
	 */
	private ColumnReader[] columnReaders;

	/**
	 * The number of rows that have been returned.
	 */
	private long rowsReturned;

	/**
	 * The number of rows that have been reading, including the current in flight row group.
	 */
	private long totalCountLoadedSoFar;

	// the index of the next row to return
	private int nextRow;

	// the number of rows in the current batch
	private int rowsInBatch;

	public ParquetColumnarRowSplitReader(
			boolean utcTimestamp,
			Configuration conf,
			LogicalType[] selectedTypes,
			String[] selectedFieldNames,
			ColumnBatchGenerator generator,
			int batchSize,
			Path path,
			long splitStart,
			long splitLength) throws IOException {
		this.utcTimestamp = utcTimestamp;
		this.selectedTypes = selectedTypes;
		this.batchSize = batchSize;
		// then we need to apply the predicate push down filter
		ParquetMetadata footer = readFooter(conf, path, range(splitStart, splitStart + splitLength));
		MessageType fileSchema = footer.getFileMetaData().getSchema();
		FilterCompat.Filter filter = getFilter(conf);
		List<BlockMetaData> blocks = filterRowGroups(filter, footer.getBlocks(), fileSchema);

		this.fileSchema = footer.getFileMetaData().getSchema();
		this.requestedSchema = clipParquetSchema(fileSchema, selectedFieldNames);
		this.reader = new ParquetFileReader(
				conf, footer.getFileMetaData(), path, blocks, requestedSchema.getColumns());

		long totalRowCount = 0;
		for (BlockMetaData block : blocks) {
			totalRowCount += block.getRowCount();
		}
		this.totalRowCount = totalRowCount;
		this.nextRow = 0;
		this.rowsInBatch = 0;
		this.rowsReturned = 0;

		checkSchema();

		this.writableVectors = createWritableVectors();
		this.columnarBatch = generator.generate(createReadableVectors());
		this.row = new ColumnarRow(columnarBatch);
	}

	/**
	 * Clips `parquetSchema` according to `fieldNames`.
	 */
	private static MessageType clipParquetSchema(GroupType parquetSchema, String[] fieldNames) {
		Type[] types = new Type[fieldNames.length];
		for (int i = 0; i < fieldNames.length; ++i) {
			String fieldName = fieldNames[i];
			if (parquetSchema.getFieldIndex(fieldName) < 0) {
				throw new IllegalArgumentException(fieldName + " does not exist");
			}
			types[i] = parquetSchema.getType(fieldName);
		}
		return Types.buildMessage().addFields(types).named("flink-parquet");
	}

	private WritableColumnVector[] createWritableVectors() {
		WritableColumnVector[] columns = new WritableColumnVector[selectedTypes.length];
		for (int i = 0; i < selectedTypes.length; i++) {
			columns[i] = createWritableColumnVector(
					batchSize,
					selectedTypes[i],
					requestedSchema.getColumns().get(i).getPrimitiveType());
		}
		return columns;
	}

	/**
	 * Create readable vectors from writable vectors.
	 * Especially for decimal, see {@link ParquetDecimalVector}.
	 */
	private ColumnVector[] createReadableVectors() {
		ColumnVector[] vectors = new ColumnVector[writableVectors.length];
		for (int i = 0; i < writableVectors.length; i++) {
			vectors[i] = selectedTypes[i].getTypeRoot() == LogicalTypeRoot.DECIMAL ?
					new ParquetDecimalVector(writableVectors[i]) :
					writableVectors[i];
		}
		return vectors;
	}

	private void checkSchema() throws IOException, UnsupportedOperationException {
		if (selectedTypes.length != requestedSchema.getFieldCount()) {
			throw new RuntimeException("The quality of field type is incompatible with the request schema!");
		}

		/*
		 * Check that the requested schema is supported.
		 */
		for (int i = 0; i < requestedSchema.getFieldCount(); ++i) {
			Type t = requestedSchema.getFields().get(i);
			if (!t.isPrimitive() || t.isRepetition(Type.Repetition.REPEATED)) {
				throw new UnsupportedOperationException("Complex types not supported.");
			}

			String[] colPath = requestedSchema.getPaths().get(i);
			if (fileSchema.containsPath(colPath)) {
				ColumnDescriptor fd = fileSchema.getColumnDescription(colPath);
				if (!fd.equals(requestedSchema.getColumns().get(i))) {
					throw new UnsupportedOperationException("Schema evolution not supported.");
				}
			} else {
				if (requestedSchema.getColumns().get(i).getMaxDefinitionLevel() == 0) {
					// Column is missing in data but the required data is non-nullable. This file is invalid.
					throw new IOException("Required column is missing in data file. Col: " + Arrays.toString(colPath));
				}
			}
		}
	}

	/**
	 * Method used to check if the end of the input is reached.
	 *
	 * @return True if the end is reached, otherwise false.
	 * @throws IOException Thrown, if an I/O error occurred.
	 */
	public boolean reachedEnd() throws IOException {
		return !ensureBatch();
	}

	public ColumnarRow nextRecord() {
		// return the next row
		row.setRowId(this.nextRow++);
		return row;
	}

	/**
	 * Checks if there is at least one row left in the batch to return. If no more row are
	 * available, it reads another batch of rows.
	 *
	 * @return Returns true if there is one more row to return, false otherwise.
	 * @throws IOException throw if an exception happens while reading a batch.
	 */
	private boolean ensureBatch() throws IOException {
		if (nextRow >= rowsInBatch) {
			// No more rows available in the Rows array.
			nextRow = 0;
			// Try to read the next batch if rows from the file.
			return nextBatch();
		}
		// there is at least one Row left in the Rows array.
		return true;
	}

	/**
	 * Advances to the next batch of rows. Returns false if there are no more.
	 */
	private boolean nextBatch() throws IOException {
		for (WritableColumnVector v : writableVectors) {
			v.reset();
		}
		columnarBatch.setNumRows(0);
		if (rowsReturned >= totalRowCount) {
			return false;
		}
		if (rowsReturned == totalCountLoadedSoFar) {
			readNextRowGroup();
		}

		int num = (int) Math.min(batchSize, totalCountLoadedSoFar - rowsReturned);
		for (int i = 0; i < columnReaders.length; ++i) {
			//noinspection unchecked
			columnReaders[i].readToVector(num, writableVectors[i]);
		}
		rowsReturned += num;
		columnarBatch.setNumRows(num);
		rowsInBatch = num;
		return true;
	}

	private void readNextRowGroup() throws IOException {
		PageReadStore pages = reader.readNextRowGroup();
		if (pages == null) {
			throw new IOException("expecting more rows but reached last block. Read "
					+ rowsReturned + " out of " + totalRowCount);
		}
		List<ColumnDescriptor> columns = requestedSchema.getColumns();
		columnReaders = new AbstractColumnReader[columns.size()];
		for (int i = 0; i < columns.size(); ++i) {
			columnReaders[i] = createColumnReader(
					utcTimestamp,
					selectedTypes[i],
					columns.get(i),
					pages.getPageReader(columns.get(i)));
		}
		totalCountLoadedSoFar += pages.getRowCount();
	}

	@Override
	public void close() throws IOException {
		if (reader != null) {
			reader.close();
			reader = null;
		}
	}

	/**
	 * Interface to gen {@link VectorizedColumnBatch}.
	 */
	public interface ColumnBatchGenerator {
		VectorizedColumnBatch generate(ColumnVector[] readVectors);
	}
}
