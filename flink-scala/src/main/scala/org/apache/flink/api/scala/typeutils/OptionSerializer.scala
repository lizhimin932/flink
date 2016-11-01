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
package org.apache.flink.api.scala.typeutils

import org.apache.flink.annotation.Internal
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.memory.{DataOutputView, DataInputView}

/**
 * Serializer for [[Option]].
 */
@Internal
class OptionSerializer[A](val elemSerializer: TypeSerializer[A])
  extends TypeSerializer[Option[A]] {

  override def duplicate: OptionSerializer[A] = {
    val duplicatedElemSerializer = elemSerializer.duplicate()

    if (duplicatedElemSerializer.eq(elemSerializer)) {
      this
    } else {
      new OptionSerializer[A](duplicatedElemSerializer)
    }
  }

  override def createInstance: Option[A] = {
    None
  }

  override def isImmutableType: Boolean = elemSerializer == null || elemSerializer.isImmutableType

  override def getLength: Int = -1

  override def copy(from: Option[A]): Option[A] = from match {
    case Some(a) => Some(elemSerializer.copy(a))
    case None => from
    case null => null
  }

  override def copy(from: Option[A], reuse: Option[A]): Option[A] = copy(from)

  override def copy(source: DataInputView, target: DataOutputView): Unit = {
    val value = source.readByte()
    target.writeByte(value)
    if (value == 1) {
      elemSerializer.copy(source, target)
    }
  }

  override def serialize(either: Option[A], target: DataOutputView): Unit = either match {
    case Some(a) =>
      target.writeByte(1)
      elemSerializer.serialize(a, target)
    case None => target.writeByte(0)
    case null => target.writeByte(2)
  }

  override def deserialize(source: DataInputView): Option[A] = {
    val value = source.readByte()
    value match {
      case 0 => None
      case 1 => Some(elemSerializer.deserialize(source))
      case 2 => null
    }
  }

  override def deserialize(reuse: Option[A], source: DataInputView): Option[A] = deserialize(source)

  override def equals(obj: Any): Boolean = {
    obj match {
      case optionSerializer: OptionSerializer[_] =>
        optionSerializer.canEqual(this) && elemSerializer.equals(optionSerializer.elemSerializer)
      case _ => false
    }
  }

  override def canEqual(obj: scala.Any): Boolean = {
    obj.isInstanceOf[OptionSerializer[_]]
  }

  override def hashCode(): Int = {
    elemSerializer.hashCode()
  }
}
