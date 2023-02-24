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

package org.apache.flink.table.runtime.functions.scalar;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;

/** Implementation of {@link BuiltInFunctionDefinitions#BIT_OR}. */
@Internal
public class BitOrFunction extends BuiltInScalarFunction {

    public BitOrFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.BIT_OR, context);
    }

    public Byte eval(Byte a, Byte b) {
        if (a == null || b == null) {
            if (a == null) {
                a = 0;
            }
            if (b == null) {
                b = 0;
            }
        }
        return (byte) (a | b);
    }

    public Short eval(Short a, Short b) {
        if (a == null || b == null) {
            if (a == null) {
                a = 0;
            }
            if (b == null) {
                b = 0;
            }
        }
        return (short) (a | b);
    }

    public Integer eval(Integer a, Integer b) {
        if (a == null || b == null) {
            if (a == null) {
                a = 0;
            }
            if (b == null) {
                b = 0;
            }
        }
        return a | b;
    }

    public Long eval(Long a, Long b) {
        if (a == null || b == null) {
            if (a == null) {
                a = 0L;
            }
            if (b == null) {
                b = 0L;
            }
        }
        return a | b;
    }
}
