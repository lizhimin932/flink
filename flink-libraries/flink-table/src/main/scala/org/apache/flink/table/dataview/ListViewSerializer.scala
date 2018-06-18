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

package org.apache.flink.table.dataview

import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base.{CollectionSerializerConfigSnapshot, ListSerializer}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.ListView
import org.apache.flink.util.Preconditions

/**
  * A serializer for [[ListView]]. The serializer relies on an element
  * serializer for the serialization of the list's elements.
  *
  * The serialization format for the list is as follows: four bytes for the length of the list,
  * followed by the serialized representation of each element.
  *
  * @param listSerializer List serializer.
  * @tparam T The type of element in the list.
  */
class ListViewSerializer[T](val listSerializer: ListSerializer[T])
  extends CompositeTypeSerializer[ListView[T]](
    new ListViewSerializerConfigSnapshot[T](Preconditions.checkNotNull(listSerializer)),
    listSerializer) {

  override def isImmutableType: Boolean = false

  override def duplicate(): TypeSerializer[ListView[T]] = {
    new ListViewSerializer[T](listSerializer.duplicate().asInstanceOf[ListSerializer[T]])
  }

  override def createInstance(): ListView[T] = {
    new ListView[T]
  }

  override def copy(from: ListView[T]): ListView[T] = {
    new ListView[T](null, listSerializer.copy(from.list))
  }

  override def copy(from: ListView[T], reuse: ListView[T]): ListView[T] = copy(from)

  override def getLength: Int = -1

  override def serialize(record: ListView[T], target: DataOutputView): Unit = {
    listSerializer.serialize(record.list, target)
  }

  override def deserialize(source: DataInputView): ListView[T] = {
    new ListView[T](null, listSerializer.deserialize(source))
  }

  override def deserialize(reuse: ListView[T], source: DataInputView): ListView[T] =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    listSerializer.copy(source, target)

  override def canEqual(obj: scala.Any): Boolean = obj != null && obj.getClass == getClass

  override def hashCode(): Int = listSerializer.hashCode()

  override def equals(obj: Any): Boolean = canEqual(this) &&
    listSerializer.equals(obj.asInstanceOf[ListViewSerializer[_]].listSerializer)

  override def isComparableSnapshot(
      configSnapshot: TypeSerializerConfigSnapshot[_]): Boolean = {

    configSnapshot.isInstanceOf[ListViewSerializerConfigSnapshot[T]] ||
      // backwards compatibility path;
      // Flink versions older or equal to 1.5.x returns a
      // CollectionSerializerConfigSnapshot as the snapshot
      configSnapshot.isInstanceOf[CollectionSerializerConfigSnapshot[_, T]]
  }
}
