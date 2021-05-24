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

package org.apache.flink.api.common.typeutils.base.array;

import org.apache.flink.api.common.typeutils.SerializerTestBase;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * A test for the {@link
 * org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer}.
 */
public class ShortPrimitiveArraySerializerTest extends SerializerTestBase<short[]> {

    @Override
    protected TypeSerializer<short[]> createSerializer() {
        return new ShortPrimitiveArraySerializer();
    }

    @Override
    protected Class<short[]> getTypeClass() {
        return short[].class;
    }

    @Override
    protected int getLength() {
        return -1;
    }

    @Override
    protected short[][] getTestData() {
        return new short[][] {
            new short[] {0, 1, 2, 3, -1, -2, -3, Short.MAX_VALUE, Short.MIN_VALUE},
            new short[] {},
            new short[] {-1, -2, 9673, 26782, 0, 0, 0}
        };
    }
}
