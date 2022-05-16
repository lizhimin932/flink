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

package org.apache.flink.table.runtime.generated;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;

/**
 * Interface for code generated hash code of {@link RowData} which will select some fields to hash
 * or {@link ArrayData} or {@link MapData}.
 *
 * <p>Due to Janino's support for generic type is not very friendly, so here can't introduce generic
 * type for {@link HashFunction}, please see https://github.com/janino-compiler/janino/issues/109
 * for details.
 */
public interface HashFunction {

    int hashCode(RowData row);

    int hashCode(ArrayData array);

    int hashCode(MapData map);
}
