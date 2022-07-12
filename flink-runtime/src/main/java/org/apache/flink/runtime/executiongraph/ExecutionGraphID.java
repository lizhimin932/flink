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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.util.AbstractID;

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

/** A class for statistically unique execution graph IDs. */
public class ExecutionGraphID extends AbstractID {

    private static final long serialVersionUID = 1L;

    public ExecutionGraphID() {
        super();
    }

    public ExecutionGraphID(byte[] bytes) {
        super(bytes);
    }

    private ExecutionGraphID(long lowerPart, long upperPart) {
        super(lowerPart, upperPart);
    }

    public void writeTo(ByteBuf buf) {
        buf.writeLong(lowerPart);
        buf.writeLong(upperPart);
    }

    public static ExecutionGraphID fromByteBuf(ByteBuf buf) {
        final long lower = buf.readLong();
        final long upper = buf.readLong();
        return new ExecutionGraphID(lower, upper);
    }
}
