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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.calcite.rex.RexNode
import org.apache.calcite.tools.RelBuilder

abstract class Expression extends TreeNode[Expression] { self: Product =>
  def name: String = Expression.freshName("expression")

  /**
    * Convert Expression to its counterpart in Calcite, i.e. RexNode
    */
  def toRexNode(implicit relBuilder: RelBuilder): RexNode =
    throw new UnsupportedOperationException(
      s"${this.getClass.getName} cannot be transformed to RexNode"
    )
}

abstract class BinaryExpression extends Expression { self: Product =>
  def left: Expression
  def right: Expression
  def children = Seq(left, right)
}

abstract class UnaryExpression extends Expression { self: Product =>
  def child: Expression
  def children = Seq(child)
}

abstract class LeafExpression extends Expression { self: Product =>
  val children = Nil
}

case class NopExpression() extends LeafExpression {
  override val name = Expression.freshName("nop")
}

object Expression {
  def freshName(prefix: String): String = {
    s"$prefix-${freshNameCounter.getAndIncrement}"
  }

  val freshNameCounter = new AtomicInteger
}
