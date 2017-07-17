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

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils._
import org.apache.flink.api.common.typeutils.base.{CollectionSerializerConfigSnapshot, ListSerializer}
import org.apache.flink.core.memory.{DataInputView, DataOutputView}
import org.apache.flink.table.api.dataview.ListView

/**
  * A serializer for {@link HeapListView}. The serializer relies on an element
  * serializer for the serialization of the list's elements.
  *
  * <p>The serialization format for the list is as follows: four bytes for the length of the lost,
  * followed by the serialized representation of each element.
  *
  * @param listSerializer List serializer.
  * @tparam T The type of element in the list.
  */
@Internal
class ListViewSerializer[T](listSerializer: ListSerializer[T])
  extends TypeSerializer[ListView[T]] {

  override def isImmutableType: Boolean = listSerializer.isImmutableType

  override def duplicate(): TypeSerializer[ListView[T]] = {
    new ListViewSerializer[T](listSerializer.duplicate().asInstanceOf[ListSerializer[T]])
  }

  override def createInstance(): ListView[T] = new HeapListView[T](listSerializer.createInstance())

  override def copy(from: ListView[T]): ListView[T] = {
    val list = from.asInstanceOf[HeapListView[T]].list
    new HeapListView[T](listSerializer.copy(list))
  }

  override def copy(from: ListView[T], reuse: ListView[T]): ListView[T] = copy(from)

  override def getLength: Int = -1

  override def serialize(record: ListView[T], target: DataOutputView): Unit = {
    val list = record.asInstanceOf[HeapListView[T]].list
    listSerializer.serialize(list, target)
  }

  override def deserialize(source: DataInputView): ListView[T] =
    new HeapListView[T](listSerializer.deserialize(source))

  override def deserialize(reuse: ListView[T], source: DataInputView): ListView[T] =
    deserialize(source)

  override def copy(source: DataInputView, target: DataOutputView): Unit =
    listSerializer.copy(source, target)

  override def canEqual(obj: scala.Any): Boolean = canEqual(this) &&
    listSerializer.equals(obj.asInstanceOf[ListSerializer[_]])

  override def hashCode(): Int = listSerializer.hashCode()

  override def equals(obj: Any): Boolean = obj != null && obj.getClass == getClass

  override def snapshotConfiguration(): TypeSerializerConfigSnapshot =
    listSerializer.snapshotConfiguration()

  // copy and modified from ListSerializer.ensureCompatibility
  override def ensureCompatibility(configSnapshot: TypeSerializerConfigSnapshot)
  : CompatibilityResult[ListView[T]] = {
    configSnapshot match {
      case snapshot: CollectionSerializerConfigSnapshot[_] =>
        val previousKvSerializersAndConfigs = snapshot.getNestedSerializersAndConfigs

        val compatResult = CompatibilityUtil.resolveCompatibilityResult(
          previousKvSerializersAndConfigs.get(0).f0,
          classOf[UnloadableDummyTypeSerializer[_]],
          previousKvSerializersAndConfigs.get(0).f1,
          listSerializer.getElementSerializer)

        if (!compatResult.isRequiresMigration) {
          CompatibilityResult.compatible[ListView[T]]
        } else if (compatResult.getConvertDeserializer != null) {
          CompatibilityResult.requiresMigration(
            new ListViewSerializer[T](
              new ListSerializer[T](
                new TypeDeserializerAdapter[T](compatResult.getConvertDeserializer))
            )
          )
        } else {
          CompatibilityResult.requiresMigration[ListView[T]]
        }

      case _ => CompatibilityResult.requiresMigration[ListView[T]]
    }
  }
}
