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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;

import org.junit.jupiter.api.Test;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for the {@link OutputTag}. */
public class OutputTagTest {

    @Test
    public void testNullRejected() {
        assertThrows(NullPointerException.class, () -> {
                    new OutputTag<Integer>(null);
        });
    }

    @Test
    public void testNullRejectedWithTypeInfo() {
        assertThrows(NullPointerException.class, () -> {
                    new OutputTag<>(null, BasicTypeInfo.INT_TYPE_INFO);
        });
    }

    @Test
    public void testEmptyStringRejected() {
        assertThrows(IllegalArgumentException.class, () -> {
                    new OutputTag<Integer>("");
        });
    }

    @Test
    public void testEmptyStringRejectedWithTypeInfo() {
        assertThrows(IllegalArgumentException.class, () -> {
                    new OutputTag<>("", BasicTypeInfo.INT_TYPE_INFO);
        });
    }
}
