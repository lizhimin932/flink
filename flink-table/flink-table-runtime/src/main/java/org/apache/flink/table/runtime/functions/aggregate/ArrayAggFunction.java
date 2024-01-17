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

package org.apache.flink.table.runtime.functions.aggregate;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.runtime.typeutils.InternalSerializers;
import org.apache.flink.table.runtime.typeutils.LinkedListSerializer;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.table.types.utils.DataTypeUtils.toInternalDataType;

/** Built-in ARRAY_AGG aggregate function. */
@Internal
public final class ArrayAggFunction<T>
        extends BuiltInAggregateFunction<ArrayData, ArrayAggFunction.ArrayAggAccumulator<T>> {

    private static final long serialVersionUID = -5860934997657147836L;

    private final transient DataType elementDataType;

    private final boolean ignoreNulls;

    public ArrayAggFunction(LogicalType elementType, boolean ignoreNulls) {
        this.elementDataType = toInternalDataType(elementType);
        this.ignoreNulls = ignoreNulls;
    }

    // --------------------------------------------------------------------------------------------
    // Planning
    // --------------------------------------------------------------------------------------------

    @Override
    public List<DataType> getArgumentDataTypes() {
        return Collections.singletonList(elementDataType);
    }

    @Override
    public DataType getAccumulatorDataType() {
        DataType linkedListType = getLinkedListType();
        return DataTypes.STRUCTURED(
                ArrayAggAccumulator.class,
                DataTypes.FIELD("list", linkedListType),
                DataTypes.FIELD("retractList", linkedListType));
    }

    @Override
    public DataType getOutputDataType() {
        return DataTypes.ARRAY(elementDataType).bridgedTo(ArrayData.class);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private DataType getLinkedListType() {
        TypeSerializer<T> serializer = InternalSerializers.create(elementDataType.getLogicalType());
        return DataTypes.RAW(
                LinkedList.class, (TypeSerializer) new LinkedListSerializer<>(serializer));
    }

    // --------------------------------------------------------------------------------------------
    // Runtime
    // --------------------------------------------------------------------------------------------

    /** Accumulator for ARRAY_AGG with retraction. */
    public static class ArrayAggAccumulator<T> {
        public LinkedList<T> list;
        public LinkedList<T> retractList;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ArrayAggAccumulator<?> that = (ArrayAggAccumulator<?>) o;
            return Objects.equals(list, that.list) && Objects.equals(retractList, that.retractList);
        }

        @Override
        public int hashCode() {
            return Objects.hash(list, retractList);
        }
    }

    @Override
    public ArrayAggAccumulator<T> createAccumulator() {
        final ArrayAggAccumulator<T> acc = new ArrayAggAccumulator<>();
        acc.list = new LinkedList<>();
        acc.retractList = new LinkedList<>();
        return acc;
    }

    public void accumulate(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value == null) {
            if (!ignoreNulls) {
                acc.list.add(null);
            }
        } else {
            acc.list.add(value);
        }
    }

    public void retract(ArrayAggAccumulator<T> acc, T value) throws Exception {
        if (value != null) {
            if (!acc.list.remove(value)) {
                acc.retractList.add(value);
            }
        }
    }

    public void merge(ArrayAggAccumulator<T> acc, Iterable<ArrayAggAccumulator<T>> its)
            throws Exception {
        for (ArrayAggAccumulator<T> otherAcc : its) {
            // merge list of acc and other
            List<T> buffer = new ArrayList<>();
            for (T element : acc.list) {
                buffer.add(element);
            }
            for (T element : otherAcc.list) {
                buffer.add(element);
            }
            // merge retract list of acc and other
            List<T> retractBuffer = new ArrayList<>();
            for (T element : acc.retractList) {
                retractBuffer.add(element);
            }
            for (T element : otherAcc.retractList) {
                retractBuffer.add(element);
            }

            // merge list & retract list
            List<T> newRetractBuffer = new ArrayList<>();
            for (T element : retractBuffer) {
                if (!buffer.remove(element)) {
                    newRetractBuffer.add(element);
                }
            }

            // update to acc
            acc.list.clear();
            acc.list.addAll(buffer);
            acc.retractList.clear();
            acc.retractList.addAll(newRetractBuffer);
        }
    }

    @Override
    public ArrayData getValue(ArrayAggAccumulator<T> acc) {
        try {
            List<T> accList = acc.list;
            if (accList == null || accList.isEmpty()) {
                // array_agg returns null rather than an empty array when there are no input rows.
                return null;
            } else {
                return new GenericArrayData(accList.toArray());
            }
        } catch (Exception e) {
            throw new FlinkRuntimeException(e);
        }
    }

    public void resetAccumulator(ArrayAggAccumulator<T> acc) {
        acc.list.clear();
        acc.retractList.clear();
    }
}
