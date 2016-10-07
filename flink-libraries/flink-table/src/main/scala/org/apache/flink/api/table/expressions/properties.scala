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

package org.apache.flink.api.table.expressions

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo
import org.apache.flink.api.table.FlinkRelBuilder.NamedProperty
import org.apache.flink.api.table.validate.{ValidationFailure, ValidationSuccess}

abstract class Property(child: Expression) extends UnaryExpression {

  override def toString = s"Property($child)"

  override private[flink] def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException("Property cannot be transformed to RexNode.")

  override private[flink] def validateInput() =
    if (child.isInstanceOf[WindowReference]) {
      ValidationSuccess
    } else {
      ValidationFailure("Child must be a window reference.")
    }

  private[flink] def toNamedProperty(name: String)(implicit relBuilder: RelBuilder)
    : NamedProperty = NamedProperty(name, this)
}

case class WindowStart(child: Expression) extends Property(child) {

  override private[flink] def resultType = SqlTimeTypeInfo.TIMESTAMP

  override def toString: String = s"start($child)"
}

case class WindowEnd(child: Expression) extends Property(child) {

  override private[flink] def resultType = SqlTimeTypeInfo.TIMESTAMP

  override def toString: String = s"end($child)"
}
