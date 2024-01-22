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

package org.apache.flink.table.runtime.operators.join.stream.bundle;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.RowDataUtil;
import org.apache.flink.types.RowKind;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

/** For the case that input has no uniqueKey. */
public class InputSideHasNoUniqueKeyBundle extends AbstractBufferBundle<RowData> {

    /**
     * The structure of the bundle: first-level key is the joinKey while the second-level key is the
     * hash value of the record. And the value of the second hash is a list of records. The bundle
     * only stores the accumulated records.When the retract record occurs it would find the
     * corresponding records(accumulated) and remove it.
     */
    private final Map<RowData, Map<Integer, List<RowData>>> bundle;

    public InputSideHasNoUniqueKeyBundle() {
        this.bundle = new HashMap<>();
        this.count = 0;
        this.actualSize = 0;
    }

    @Override
    public void clear() {
        count = 0;
        actualSize = 0;
        bundle.clear();
    }

    @Override
    public int addRecord(RowData joinKey, @Nullable RowData uniqueKey, RowData record) {
        bundle.computeIfAbsent(joinKey, k -> new HashMap<>());

        RowKind rowKind = record.getRowKind();
        record.setRowKind(RowKind.INSERT);
        int hashKey = record.hashCode();
        record.setRowKind(rowKind);

        if (!foldRecord(joinKey, hashKey, record)) {
            actualSize++;
            bundle.computeIfAbsent(joinKey, k -> new HashMap<>())
                    .computeIfAbsent(hashKey, k -> new ArrayList<>())
                    .add(record);
        }
        return ++count;
    }

    @Override
    public Map<RowData, List<RowData>> getRecords() throws Exception {
        Map<RowData, List<RowData>> result = new HashMap<>();
        for (Map.Entry<RowData, Map<Integer, List<RowData>>> entry : bundle.entrySet()) {
            result.computeIfAbsent(entry.getKey(), k -> new ArrayList<>());
            for (List<RowData> list : entry.getValue().values()) {
                result.get(entry.getKey()).addAll(list);
            }
        }
        return result;
    }

    @Override
    public Map<RowData, List<RowData>> getRecordsWithJoinKey(RowData joinKey) {
        throw new UnsupportedOperationException(
                "JoinKeyContainsUniqueKeyBundle do not support getRecordsWithJoinKey() function.");
    }

    /**
     * Fold the records only in accumulate and retract modes. The rule:
     *
     * <ol>
     *   <li>the input is accumulateMsg -> check if there is retractMsg before in the same hashKey
     *       if yes then fold that else add input to the bundle.
     *   <li>the input is retractMsg -> remove the accumulateMsg in the same HashKey from bundle.
     *       (The same HashKey means that the input's field values are completely equivalent.)
     * </ol>
     *
     * <p>In this context, the symbols refer to the following RowKind values: accumulateMsg refers
     * to +I/+U which refers to {@link RowKind#INSERT}/{@link RowKind#UPDATE_AFTER}. retractMsg
     * refers to -U/-D which refers to {@link RowKind#UPDATE_BEFORE}/{@link RowKind#DELETE}.
     */
    private boolean foldRecord(RowData joinKey, int hashKey, RowData record) {
        List<RowData> list = bundle.get(joinKey).computeIfAbsent(hashKey, k -> new ArrayList<>());
        ListIterator<RowData> iterator = list.listIterator(bundle.get(joinKey).get(hashKey).size());
        while (iterator.hasPrevious()) {
            RowData rec = iterator.previous();
            if ((RowDataUtil.isAccumulateMsg(record) && RowDataUtil.isRetractMsg(rec))
                    || (RowDataUtil.isRetractMsg(record) && RowDataUtil.isAccumulateMsg(rec))) {
                iterator.remove();
                actualSize--;
                if (list.isEmpty()) {
                    bundle.get(joinKey).remove(hashKey);
                    if (bundle.get(joinKey).isEmpty()) {
                        bundle.remove(joinKey);
                    }
                }
                return true;
            }
        }
        return false;
    }
}
