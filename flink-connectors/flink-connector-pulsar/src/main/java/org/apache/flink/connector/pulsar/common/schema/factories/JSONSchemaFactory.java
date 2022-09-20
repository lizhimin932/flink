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

package org.apache.flink.connector.pulsar.common.schema.factories;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.AvroUtils;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.decodeClassInfo;

/** The schema factory for pulsar's {@link JSONSchema}. */
public class JSONSchemaFactory<T> extends BaseStructSchemaFactory<T> {

    @Override
    public SchemaType type() {
        return SchemaType.JSON;
    }

    @Override
    public Schema<T> createSchema(SchemaInfo info) {
        Class<T> typeClass = decodeClassInfo(info);
        return JSONSchema.of(typeClass, info.getProperties());
    }

    @Override
    public TypeInformation<T> createTypeInfo(SchemaInfo info) {
        try {
            Class<T> decodeClassInfo = decodeClassInfo(info);
            return AvroUtils.getAvroUtils().createAvroTypeInfo(decodeClassInfo);
        } catch (Exception e) {
            return super.createTypeInfo(info);
        }
    }
}
