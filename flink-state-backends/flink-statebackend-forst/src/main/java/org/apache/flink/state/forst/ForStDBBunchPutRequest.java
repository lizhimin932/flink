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

package org.apache.flink.state.forst;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.state.InternalStateFuture;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.io.IOException;
import java.util.Map;

import static org.apache.flink.state.forst.ForStDBIterRequest.startWithKeyPrefix;

/**
 * The Bunch Put access request for ForStDB.
 *
 * @param <K> The type of key in original state access request.
 */
public class ForStDBBunchPutRequest<K> extends ForStDBPutRequest<ContextKey<K>, Map<?, ?>> {

    /** Serializer for the user values. */
    final TypeSerializer<Object> userValueSerializer;

    /** The data outputStream used for value serializer, which should be thread-safe. */
    final ThreadLocal<DataOutputSerializer> valueSerializerView;

    /** The data inputStream used for value deserializer, which should be thread-safe. */
    final ThreadLocal<DataInputDeserializer> valueDeserializerView;

    final int keyGroupPrefixBytes;

    public ForStDBBunchPutRequest(
            ContextKey<K> key, Map value, ForStMapState table, InternalStateFuture<Void> future) {
        super(key, value, table, future);
        this.userValueSerializer = table.userValueSerializer;
        this.valueSerializerView = table.valueSerializerView;
        this.valueDeserializerView = table.valueDeserializerView;
        this.keyGroupPrefixBytes = table.getKeyGroupPrefixBytes();
    }

    @Override
    public void process(ForStDBWriteBatchWrapper writeBatchWrapper, RocksDB db)
            throws IOException, RocksDBException {
        if (value == null) {
            byte[] prefix = buildSerializedKey(null);
            try (RocksIterator iter = db.newIterator(table.getColumnFamilyHandle())) {
                iter.seek(prefix);
                while (iter.isValid()) {
                    byte[] rocksKey = iter.key();
                    if (startWithKeyPrefix(prefix, rocksKey, keyGroupPrefixBytes)) {
                        writeBatchWrapper.remove(table.getColumnFamilyHandle(), rocksKey);
                    } else {
                        break;
                    }
                    iter.next();
                }
            }
        } else {
            for (Map.Entry<?, ?> entry : value.entrySet()) {
                byte[] key = buildSerializedKey(entry.getKey());
                byte[] value = buildSerializedValue(entry.getValue());
                writeBatchWrapper.put(table.getColumnFamilyHandle(), key, value);
            }
        }
    }

    public byte[] buildSerializedKey(Object userKey) throws IOException {
        key.setUserKey(userKey);
        return table.serializeKey(key);
    }

    public byte[] buildSerializedValue(Object singleValue) throws IOException {
        DataOutputSerializer outputView = valueSerializerView.get();
        outputView.clear();
        userValueSerializer.serialize(singleValue, outputView);
        return outputView.getCopyOfBuffer();
    }

    // --------------- For testing usage ---------------
    @VisibleForTesting
    public Map<?, ?> getBunchValue() {
        return value;
    }
}
