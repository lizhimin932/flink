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

package org.apache.flink.formats.parquet.avro;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.formats.parquet.ParquetBuilder;
import org.apache.flink.formats.parquet.ParquetWriterFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;

/**
 * Convenience builder to create {@link ParquetWriterFactory} instances for the different Avro
 * types.
 */
@Experimental
public class AvroParquetWriters {
    /**
     * Creates a ParquetWriterFactory for an Avro specific type. The Parquet writers will use the
     * schema of that specific type to build and write the columnar data.
     *
     * @param type The class of the type to write.
     */
    public static <T extends SpecificRecordBase> ParquetWriterFactory<T> forSpecificRecord(
            Class<T> type) {
        final Schema schema = SpecificData.get().getSchema(type);
        final SpecificData specificData = SpecificData.getForClass(type);
        final ParquetBuilder<T> builder =
                (out) -> createAvroParquetWriter(schema, specificData, out);
        return new ParquetWriterFactory<>(builder);
    }

    /**
     * Creates a ParquetWriterFactory that accepts and writes Avro generic types. The Parquet
     * writers will use the given schema to build and write the columnar data.
     *
     * @param schema The schema of the generic type.
     */
    public static ParquetWriterFactory<GenericRecord> forGenericRecord(Schema schema) {
        final ParquetBuilder<GenericRecord> builder =
                // Must override the lambda representation because of a bug in shading lambda
                // serialization, see similar issue FLINK-28043 for more details.
                new ParquetBuilder<GenericRecord>() {
                    @Override
                    public ParquetWriter<GenericRecord> createWriter(OutputFile out)
                            throws IOException {
                        return createAvroParquetWriter(schema, GenericData.get(), out);
                    }
                };
        return new ParquetWriterFactory<>(builder);
    }

    /**
     * Creates a ParquetWriterFactory for the given type. The Parquet writers will use Avro to
     * reflectively create a schema for the type and use that schema to write the columnar data.
     *
     * @param type The class of the type to write.
     */
    public static <T> ParquetWriterFactory<T> forReflectRecord(Class<T> type) {
        final Schema schema = ReflectData.get().getSchema(type);
        final ParquetBuilder<T> builder =
                (out) -> createAvroParquetWriter(schema, ReflectData.get(), out);
        return new ParquetWriterFactory<>(builder);
    }

    private static <T> ParquetWriter<T> createAvroParquetWriter(
            Schema schema, GenericData dataModel, OutputFile out) throws IOException {

        return AvroParquetWriter.<T>builder(out)
                .withSchema(schema)
                .withDataModel(dataModel)
                .build();
    }

    // ------------------------------------------------------------------------

    /** Class is not meant to be instantiated. */
    private AvroParquetWriters() {}
}
