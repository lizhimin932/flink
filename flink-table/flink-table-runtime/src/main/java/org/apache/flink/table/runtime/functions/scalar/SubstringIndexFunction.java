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
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.SpecializedFunction;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.commons.lang3.StringUtils;

import javax.annotation.Nullable;

import java.util.Arrays;

/** Implementation of {@link BuiltInFunctionDefinitions#SUBSTRING_INDEX}. */
@Internal
public class SubstringIndexFunction extends BuiltInScalarFunction {
    public SubstringIndexFunction(SpecializedFunction.SpecializedContext context) {
        super(BuiltInFunctionDefinitions.SUBSTRING_INDEX, context);
    }

    public @Nullable StringData eval(
            @Nullable StringData expr, @Nullable StringData delim, @Nullable Integer count) {
        try {
            if (expr == null || delim == null || count == null) {
                return null;
            }

            if (count == 0) {
                return BinaryStringData.EMPTY_UTF8;
            }
            String str = expr.toString();
            if (str.isEmpty()) {
                return BinaryStringData.EMPTY_UTF8;
            }
            String delimiter = delim.toString();
            if (delimiter.isEmpty()) {
                return BinaryStringData.EMPTY_UTF8;
            }

            if (count > 0) {
                // get index of the count-th occurrences of delimiter
                int idx = StringUtils.ordinalIndexOf(str, delimiter, count);
                if (idx != StringUtils.INDEX_NOT_FOUND) {
                    return StringData.fromString(str.substring(0, idx));
                }
            } else {
                // get index of the last -count-th occurrences of delimiter
                int idx = StringUtils.lastOrdinalIndexOf(str, delimiter, -count);
                if (idx != StringUtils.INDEX_NOT_FOUND) {
                    return StringData.fromString(str.substring(idx + delimiter.length()));
                }
            }

            // can not find enough delimiter
            return expr;

        } catch (Throwable t) {
            throw new FlinkRuntimeException(t);
        }
    }

    public @Nullable byte[] eval(
            @Nullable byte[] expr, @Nullable byte[] delim, @Nullable Integer count) {
        if (expr == null || delim == null || count == null) {
            return null;
        }
        if (expr.length == 0 || delim.length == 0 || count == 0) {
            return new byte[] {};
        }

        if (count > 0) {
            int idx = -1;
            while (count > 0) {
                // get index of the next occurrence of delim
                idx = find(expr, idx + 1, delim);
                if (idx >= 0) {
                    --count;
                } else {
                    // can not find enough delim
                    return expr;
                }
            }
            if (idx == 0) {
                return new byte[] {};
            }
            return Arrays.copyOfRange(expr, 0, idx);

        } else {
            int idx = expr.length - delim.length + 1;
            while (count < 0) {
                // get index of the previous occurrence of delim
                idx = rfind(expr, idx - 1, delim);
                if (idx >= 0) {
                    ++count;
                } else {
                    // can not find enough delim
                    return expr;
                }
            }
            if (idx + delim.length == expr.length) {
                return new byte[] {};
            }
            return Arrays.copyOfRange(expr, idx + delim.length, expr.length);
        }
    }

    private int find(byte[] str, int beginIdx, byte[] target) {
        final int endIdx = str.length - target.length;
        while (beginIdx < endIdx) {
            if (match(str, beginIdx, target)) {
                return beginIdx;
            }
            ++beginIdx;
        }
        return -1;
    }

    private int rfind(byte[] str, int beginIdx, byte[] target) {
        while (beginIdx >= 0) {
            if (match(str, beginIdx, target)) {
                return beginIdx;
            }
            --beginIdx;
        }
        return -1;
    }

    private boolean match(byte[] str, int beginIdx, byte[] target) {
        for (int i = 0; i < target.length; ++i) {
            if (str[beginIdx + i] != target[i]) {
                return false;
            }
        }
        return true;
    }
}
