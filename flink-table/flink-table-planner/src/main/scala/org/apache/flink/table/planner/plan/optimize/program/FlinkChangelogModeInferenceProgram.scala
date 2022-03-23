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

package org.apache.flink.table.planner.plan.optimize.program

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.UpsertMaterialize
import org.apache.flink.table.catalog.{ManagedTableListener, ResolvedCatalogBaseTable}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.planner.plan.`trait`.UpdateKindTrait.{BEFORE_AND_AFTER, ONLY_UPDATE_AFTER, beforeAfterOrNone, onlyAfterOrNone}
import org.apache.flink.table.planner.plan.`trait`._
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.nodes.physical.stream._
import org.apache.flink.table.planner.plan.utils.RankProcessStrategy.{AppendFastStrategy, RetractStrategy, UpdateFastStrategy}
import org.apache.flink.table.planner.plan.utils._
import org.apache.flink.table.planner.sinks.DataStreamTableSink
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig
import org.apache.flink.table.runtime.operators.join.FlinkJoinType
import org.apache.flink.table.sinks.{AppendStreamTableSink, RetractStreamTableSink, StreamTableSink, UpsertStreamTableSink}
import org.apache.flink.types.RowKind

import org.apache.calcite.rel.RelNode
import org.apache.calcite.util.ImmutableBitSet

import scala.collection.JavaConversions._

/**
 * An optimize program to infer ChangelogMode for every physical node.
 */
class FlinkChangelogModeInferenceProgram extends FlinkOptimizeProgram[StreamOptimizeContext] {

  override def optimize(
      root: RelNode,
      context: StreamOptimizeContext): RelNode = {
    // step1: satisfy ModifyKindSet trait
    val physicalRoot = root.asInstanceOf[StreamPhysicalRel]
    val rootWithModifyKindSet = new SatisfyModifyKindSetTraitVisitor().visit(
      physicalRoot,
      // we do not propagate the ModifyKindSet requirement and requester among blocks
      // set default ModifyKindSet requirement and requester for root
      ModifyKindSetTrait.ALL_CHANGES,
      "ROOT")

    // step2: satisfy UpdateKind trait
    val rootModifyKindSet = getModifyKindSet(rootWithModifyKindSet)
    // use the required UpdateKindTrait from parent blocks
    val requiredUpdateKindTraits = if (rootModifyKindSet.contains(ModifyKind.UPDATE)) {
      if (context.isUpdateBeforeRequired) {
        Seq(UpdateKindTrait.BEFORE_AND_AFTER)
      } else {
        // update_before is not required, and input contains updates
        // try ONLY_UPDATE_AFTER first, and then BEFORE_AND_AFTER
        Seq(UpdateKindTrait.ONLY_UPDATE_AFTER, UpdateKindTrait.BEFORE_AND_AFTER)
      }
    } else {
      // there is no updates
      Seq(UpdateKindTrait.NONE)
    }

    val updateKindTraitVisitor = new SatisfyUpdateKindTraitVisitor(context)
    val finalRoot = requiredUpdateKindTraits.flatMap { requiredUpdateKindTrait =>
      updateKindTraitVisitor.visit(rootWithModifyKindSet, requiredUpdateKindTrait)
    }

    // step3: sanity check and return non-empty root
    if (finalRoot.isEmpty) {
      val plan = FlinkRelOptUtil.toString(root, withChangelogTraits = true)
      throw new TableException(
        "Can't generate a valid execution plan for the given query:\n" + plan)
    } else {
      finalRoot.head
    }
  }


  /**
   * A visitor which will try to satisfy the required [[ModifyKindSetTrait]] from root.
   *
   * <p>After traversed by this visitor, every node should have a correct [[ModifyKindSetTrait]]
   * or an exception should be thrown if the planner doesn't support to satisfy the required
   * [[ModifyKindSetTrait]].
   */
  private class SatisfyModifyKindSetTraitVisitor {

    /**
     * Try to satisfy the required [[ModifyKindSetTrait]] from root.
     *
     * <p>Each node should first require a [[ModifyKindSetTrait]] to its children.
     * If the trait provided by children does not satisfy the required one,
     * it should throw an exception and prompt the user that plan is not supported.
     * The required [[ModifyKindSetTrait]] may come from the node's parent,
     * or come from the node itself, depending on whether the node will destroy
     * the trait provided by children or pass the trait from children.
     *
     * <p>Each node should provide [[ModifyKindSetTrait]] according to current node's behavior
     * and the ModifyKindSetTrait provided by children.
     *
     * @param rel the node who should satisfy the requiredTrait
     * @param requiredTrait the required ModifyKindSetTrait
     * @param requester the requester who starts the requirement, used for better exception message
     * @return A converted node which satisfy required traits by inputs node of current node.
     *         Or throws exception if required trait can’t be satisfied.
     */
    def visit(
        rel: StreamPhysicalRel,
        requiredTrait: ModifyKindSetTrait,
        requester: String): StreamPhysicalRel = rel match {
      case sink: StreamPhysicalSink =>
        val name = s"Table sink '${sink.contextResolvedTable.getIdentifier.asSummaryString()}'"
        val queryModifyKindSet = deriveQueryDefaultChangelogMode(sink.getInput, name)
        val sinkRequiredTrait = ModifyKindSetTrait.fromChangelogMode(
          sink.tableSink.getChangelogMode(queryModifyKindSet))
        val children = visitChildren(sink, sinkRequiredTrait, name)
        val sinkTrait = sink.getTraitSet.plus(ModifyKindSetTrait.EMPTY)
        // ignore required trait from context, because sink is the true root
        sink.copy(sinkTrait, children).asInstanceOf[StreamPhysicalRel]

      case sink: StreamPhysicalLegacySink[_] =>
        val (sinkRequiredTrait, name) = sink.sink match {
          case _: UpsertStreamTableSink[_] =>
            (ModifyKindSetTrait.ALL_CHANGES, "UpsertStreamTableSink")
          case _: RetractStreamTableSink[_] =>
            (ModifyKindSetTrait.ALL_CHANGES, "RetractStreamTableSink")
          case _: AppendStreamTableSink[_] =>
            (ModifyKindSetTrait.INSERT_ONLY, "AppendStreamTableSink")
          case _: StreamTableSink[_] =>
            (ModifyKindSetTrait.INSERT_ONLY, "StreamTableSink")
          case ds: DataStreamTableSink[_] =>
            if (ds.withChangeFlag) {
              (ModifyKindSetTrait.ALL_CHANGES, "toRetractStream")
            } else {
              (ModifyKindSetTrait.INSERT_ONLY, "toAppendStream")
            }
          case _ =>
            throw new UnsupportedOperationException(
              s"Unsupported sink '${sink.sink.getClass.getSimpleName}'")
        }
        val children = visitChildren(sink, sinkRequiredTrait, name)
        val sinkTrait = sink.getTraitSet.plus(ModifyKindSetTrait.EMPTY)
        // ignore required trait from context, because sink is the true root
        sink.copy(sinkTrait, children).asInstanceOf[StreamPhysicalRel]

      case deduplicate: StreamPhysicalDeduplicate =>
        // deduplicate only support insert only as input
        val children = visitChildren(deduplicate, ModifyKindSetTrait.INSERT_ONLY)
        val providedTrait = if (!deduplicate.keepLastRow && !deduplicate.isRowtime) {
          // only proctime first row deduplicate does not produce UPDATE changes
          ModifyKindSetTrait.INSERT_ONLY
        } else {
          // other deduplicate produce update changes
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(deduplicate, children, providedTrait, requiredTrait, requester)

      case agg: StreamPhysicalGroupAggregate =>
        // agg support all changes in input
        val children = visitChildren(agg, ModifyKindSetTrait.ALL_CHANGES)
        val inputModifyKindSet = getModifyKindSet(children.head)
        val builder = ModifyKindSet.newBuilder()
          .addContainedKind(ModifyKind.INSERT)
          .addContainedKind(ModifyKind.UPDATE)
        if (inputModifyKindSet.contains(ModifyKind.UPDATE) ||
            inputModifyKindSet.contains(ModifyKind.DELETE)) {
          builder.addContainedKind(ModifyKind.DELETE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(agg, children, providedTrait, requiredTrait, requester)

      case tagg: StreamPhysicalGroupTableAggregateBase =>
        // table agg support all changes in input
        val children = visitChildren(tagg, ModifyKindSetTrait.ALL_CHANGES)
        // table aggregate will produce all changes, including deletions
        createNewNode(tagg, children, ModifyKindSetTrait.ALL_CHANGES, requiredTrait, requester)

      case agg: StreamPhysicalPythonGroupAggregate =>
        // agg support all changes in input
        val children = visitChildren(agg, ModifyKindSetTrait.ALL_CHANGES)
        val inputModifyKindSet = getModifyKindSet(children.head)
        val builder = ModifyKindSet.newBuilder()
          .addContainedKind(ModifyKind.INSERT)
          .addContainedKind(ModifyKind.UPDATE)
        if (inputModifyKindSet.contains(ModifyKind.UPDATE) ||
          inputModifyKindSet.contains(ModifyKind.DELETE)) {
          builder.addContainedKind(ModifyKind.DELETE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(agg, children, providedTrait, requiredTrait, requester)

      case window: StreamPhysicalGroupWindowAggregateBase =>
        // WindowAggregate and WindowTableAggregate support all changes in input
        val children = visitChildren(window, ModifyKindSetTrait.ALL_CHANGES)
        val builder = ModifyKindSet.newBuilder()
          .addContainedKind(ModifyKind.INSERT)
        if (window.emitStrategy.produceUpdates) {
          builder.addContainedKind(ModifyKind.UPDATE)
        }
        val providedTrait = new ModifyKindSetTrait(builder.build())
        createNewNode(window, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalWindowAggregate | _: StreamPhysicalWindowRank |
           _: StreamPhysicalWindowDeduplicate =>
        // WindowAggregate, WindowRank, WindowDeduplicate support insert-only in input
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        val providedTrait = ModifyKindSetTrait.INSERT_ONLY
        createNewNode(rel, children, providedTrait, requiredTrait, requester)

      case limit: StreamPhysicalLimit =>
        // limit support all changes in input
        val children = visitChildren(limit, ModifyKindSetTrait.ALL_CHANGES)
        val providedTrait = if (getModifyKindSet(children.head).isInsertOnly) {
          ModifyKindSetTrait.INSERT_ONLY
        } else {
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(limit, children, providedTrait, requiredTrait, requester)

      case _: StreamPhysicalRank | _: StreamPhysicalSortLimit =>
        // Rank and SortLimit supports consuming all changes
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        createNewNode(rel, children, ModifyKindSetTrait.ALL_CHANGES, requiredTrait, requester)

      case sort: StreamPhysicalSort =>
        // Sort supports consuming all changes
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        // Sort will buffer all inputs, and produce insert-only messages when input is finished
        createNewNode(sort, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case cep: StreamPhysicalMatch =>
        // CEP only supports consuming insert-only and producing insert-only changes
        // give a better requester name for exception message
        val children = visitChildren(cep, ModifyKindSetTrait.INSERT_ONLY, "Match Recognize")
        createNewNode(cep, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case _: StreamPhysicalTemporalSort | _: StreamPhysicalIntervalJoin |
           _: StreamPhysicalOverAggregate | _: StreamPhysicalPythonOverAggregate =>
        // TemporalSort, OverAggregate, IntervalJoin only support consuming insert-only
        // and producing insert-only changes
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(rel, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case join: StreamPhysicalJoin =>
        // join support all changes in input
        val children = visitChildren(rel, ModifyKindSetTrait.ALL_CHANGES)
        val leftKindSet = getModifyKindSet(children.head)
        val rightKindSet = getModifyKindSet(children.last)
        val innerOrSemi = join.joinSpec.getJoinType == FlinkJoinType.INNER ||
            join.joinSpec.getJoinType == FlinkJoinType.SEMI
        val providedTrait = if (innerOrSemi) {
          // forward left and right modify operations
          new ModifyKindSetTrait(leftKindSet.union(rightKindSet))
        } else {
          // otherwise, it may produce any kinds of changes
          ModifyKindSetTrait.ALL_CHANGES
        }
        createNewNode(join, children, providedTrait, requiredTrait, requester)

      case windowJoin: StreamPhysicalWindowJoin =>
        // Currently, window join only supports INSERT_ONLY in input
        val children = visitChildren(rel, ModifyKindSetTrait.INSERT_ONLY)
        createNewNode(
          windowJoin, children, ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case temporalJoin: StreamPhysicalTemporalJoin =>
        // currently, temporal join supports all kings of changes, including right side
        val children = visitChildren(temporalJoin, ModifyKindSetTrait.ALL_CHANGES)
        // forward left input changes
        val leftTrait = children.head.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        createNewNode(temporalJoin, children, leftTrait, requiredTrait, requester)

      case _: StreamPhysicalCalcBase | _: StreamPhysicalCorrelateBase |
           _: StreamPhysicalLookupJoin | _: StreamPhysicalExchange |
           _: StreamPhysicalExpand | _: StreamPhysicalMiniBatchAssigner |
           _: StreamPhysicalWatermarkAssigner | _: StreamPhysicalWindowTableFunction =>
        // transparent forward requiredTrait to children
        val children = visitChildren(rel, requiredTrait, requester)
        val childrenTrait = children.head.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        // forward children mode
        createNewNode(rel, children, childrenTrait, requiredTrait, requester)

      case union: StreamPhysicalUnion =>
        // transparent forward requiredTrait to children
        val children = visitChildren(rel, requiredTrait, requester)
        // union provides all possible kinds of children have
        val providedKindSet = ModifyKindSet.union(children.map(getModifyKindSet): _*)
        createNewNode(
          union, children, new ModifyKindSetTrait(providedKindSet), requiredTrait, requester)

      case normalize: StreamPhysicalChangelogNormalize =>
        // changelog normalize support update&delete input
        val children = visitChildren(normalize, ModifyKindSetTrait.ALL_CHANGES)
        // changelog normalize will output all changes
        val providedTrait = ModifyKindSetTrait.ALL_CHANGES
        createNewNode(normalize, children, providedTrait, requiredTrait, requester)

      case ts: StreamPhysicalTableSourceScan =>
        // ScanTableSource supports produces updates and deletions
        val providedTrait = ModifyKindSetTrait.fromChangelogMode(ts.tableSource.getChangelogMode)
        createNewNode(ts, List(), providedTrait, requiredTrait, requester)

      case _: StreamPhysicalDataStreamScan | _: StreamPhysicalLegacyTableSourceScan |
           _: StreamPhysicalValues =>
        // DataStream, TableSource and Values only support producing insert-only messages
        createNewNode(rel, List(), ModifyKindSetTrait.INSERT_ONLY, requiredTrait, requester)

      case scan: StreamPhysicalIntermediateTableScan =>
        val providedTrait = new ModifyKindSetTrait(scan.intermediateTable.modifyKindSet)
        createNewNode(scan, List(), providedTrait, requiredTrait, requester)

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported visit for ${rel.getClass.getSimpleName}")
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: ModifyKindSetTrait): List[StreamPhysicalRel] = {
      visitChildren(parent, requiredChildrenTrait, getNodeName(parent))
    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: ModifyKindSetTrait,
        requester: String): List[StreamPhysicalRel] = {
      val newChildren = for (i <- 0 until parent.getInputs.size()) yield {
        visitChild(parent, i, requiredChildrenTrait, requester)
      }
      newChildren.toList
    }

    private def visitChild(
        parent: StreamPhysicalRel,
        childOrdinal: Int,
        requiredChildTrait: ModifyKindSetTrait,
        requester: String): StreamPhysicalRel = {
      val child = parent.getInput(childOrdinal).asInstanceOf[StreamPhysicalRel]
      this.visit(child, requiredChildTrait, requester)
    }

    private def getNodeName(rel: StreamPhysicalRel): String = {
      val prefix = "StreamExec"
      val typeName = rel.getRelTypeName
      if (typeName.startsWith(prefix)) {
        typeName.substring(prefix.length)
      } else {
        typeName
      }
    }

    /**
     * Derives the [[ModifyKindSetTrait]] of query plan without required ModifyKindSet validation.
     */
    private def deriveQueryDefaultChangelogMode(
        queryNode: RelNode, name: String): ChangelogMode = {
      val newNode = visit(
        queryNode.asInstanceOf[StreamPhysicalRel],
        ModifyKindSetTrait.ALL_CHANGES,
        name)
      getModifyKindSet(newNode).toChangelogMode
    }

    private def createNewNode(
        node: StreamPhysicalRel,
        children: List[StreamPhysicalRel],
        providedTrait: ModifyKindSetTrait,
        requiredTrait: ModifyKindSetTrait,
        requestedOwner: String): StreamPhysicalRel = {
      if (!providedTrait.satisfies(requiredTrait)) {
        val diff = providedTrait.modifyKindSet.minus(requiredTrait.modifyKindSet)
        val diffString = diff.getContainedKinds
          .toList.sorted // for deterministic error message
          .map(_.toString.toLowerCase)
          .mkString(" and ")
        // creates a new node based on the new children, to have a more correct node description
        // e.g. description of GroupAggregate is based on the ModifyKindSetTrait of children
        val tempNode = node.copy(node.getTraitSet, children).asInstanceOf[StreamPhysicalRel]
        val nodeString = tempNode.getRelDetailedDescription
        throw new TableException(
          s"$requestedOwner doesn't support consuming $diffString changes " +
          s"which is produced by node $nodeString")
      }
      val newTraitSet = node.getTraitSet.plus(providedTrait)
      node.copy(newTraitSet, children).asInstanceOf[StreamPhysicalRel]
    }
  }

  /**
   * A visitor which will try to satisfy the required [[UpdateKindTrait]] from root.
   *
   * <p>After traversed by this visitor, every node should have a correct [[UpdateKindTrait]]
   * or returns None if the planner doesn't support to satisfy the required [[UpdateKindTrait]].
   */
  private class SatisfyUpdateKindTraitVisitor(private val context: StreamOptimizeContext) {
    /**
     * Try to satisfy the required [[UpdateKindTrait]] from root.
     *
     * <p>Each node will first require a UpdateKindTrait to its children.
     * The required UpdateKindTrait may come from the node's parent,
     * or come from the node itself, depending on whether the node will destroy
     * the trait provided by children or pass the trait from children.
     *
     * <p>If the node will pass the children's UpdateKindTrait without destroying it,
     * then return a new node with new inputs and forwarded UpdateKindTrait.
     *
     * <p>If the node will destroy the children's UpdateKindTrait, then the node itself
     * needs to be converted, or a new node should be generated to satisfy the required trait,
     * such as marking itself not to generate UPDATE_BEFORE,
     * or generating a new node to filter UPDATE_BEFORE.
     *
     * @param rel the node who should satisfy the requiredTrait
     * @param requiredTrait the required UpdateKindTrait
     * @return A converted node which satisfies required traits by input nodes of current node.
     *         Or None if required traits cannot be satisfied.
     */
    def visit(
        rel: StreamPhysicalRel,
        requiredTrait: UpdateKindTrait): Option[StreamPhysicalRel] = rel match {
      case sink: StreamPhysicalSink =>
        val sinkRequiredTraits = inferSinkRequiredTraits(sink)
        val upsertMaterialize = analyzeUpsertMaterializeStrategy(sink)
        visitSink(sink.copy(upsertMaterialize), sinkRequiredTraits)

      case sink: StreamPhysicalLegacySink[_] =>
        val childModifyKindSet = getModifyKindSet(sink.getInput)
        val onlyAfter = onlyAfterOrNone(childModifyKindSet)
        val beforeAndAfter = beforeAfterOrNone(childModifyKindSet)
        val sinkRequiredTraits = sink.sink match {
          case _: UpsertStreamTableSink[_] =>
            // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
            Seq(onlyAfter, beforeAndAfter)
          case _: RetractStreamTableSink[_] =>
            Seq(beforeAndAfter)
          case _: AppendStreamTableSink[_] | _: StreamTableSink[_] =>
            Seq(UpdateKindTrait.NONE)
          case ds: DataStreamTableSink[_] =>
            if (ds.withChangeFlag) {
              if (ds.needUpdateBefore) {
                Seq(beforeAndAfter)
              } else {
                // support both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
                Seq(onlyAfter, beforeAndAfter)
              }
            } else {
              Seq(UpdateKindTrait.NONE)
            }
        }
        visitSink(sink, sinkRequiredTraits)

      case _: StreamPhysicalGroupAggregate | _: StreamPhysicalGroupTableAggregate |
           _: StreamPhysicalLimit | _: StreamPhysicalPythonGroupAggregate |
           _: StreamPhysicalPythonGroupTableAggregate |
           _: StreamPhysicalGroupWindowAggregateBase =>
        // Aggregate, TableAggregate, Limit and GroupWindowAggregate requires update_before if
        // there are updates
        val requiredChildTrait = beforeAfterOrNone(getModifyKindSet(rel.getInput(0)))
        val children = visitChildren(rel, requiredChildTrait)
        // use requiredTrait as providedTrait, because they should support all kinds of UpdateKind
        createNewNode(rel, children, requiredTrait)

      case _: StreamPhysicalWindowAggregate | _: StreamPhysicalWindowRank |
           _: StreamPhysicalWindowDeduplicate | _: StreamPhysicalDeduplicate |
           _: StreamPhysicalTemporalSort | _: StreamPhysicalMatch |
           _: StreamPhysicalOverAggregate | _: StreamPhysicalIntervalJoin |
           _: StreamPhysicalPythonOverAggregate | _: StreamPhysicalWindowJoin =>
        // WindowAggregate, WindowTableAggregate, WindowRank, WindowDeduplicate, Deduplicate,
        // TemporalSort, CEP, OverAggregate, and IntervalJoin, WindowJoin require nothing about
        // UpdateKind.
        val children = visitChildren(rel, UpdateKindTrait.NONE)
        createNewNode(rel, children, requiredTrait)

      case rank: StreamPhysicalRank =>
        val rankStrategies = RankProcessStrategy.analyzeRankProcessStrategies(
          rank, rank.partitionKey, rank.orderKey)
        visitRankStrategies(rankStrategies, requiredTrait, rankStrategy => rank.copy(rankStrategy))

      case sortLimit: StreamPhysicalSortLimit =>
        val rankStrategies = RankProcessStrategy.analyzeRankProcessStrategies(
          sortLimit, ImmutableBitSet.of(), sortLimit.getCollation)
        visitRankStrategies(
          rankStrategies,
          requiredTrait,
          rankStrategy => sortLimit.copy(rankStrategy))

      case sort: StreamPhysicalSort =>
        val requiredChildTrait = beforeAfterOrNone(getModifyKindSet(sort.getInput))
        val children = visitChildren(sort, requiredChildTrait)
        createNewNode(sort, children, requiredTrait)

      case join: StreamPhysicalJoin =>
        val onlyAfterByParent = requiredTrait.updateKind == UpdateKind.ONLY_UPDATE_AFTER
        val children = join.getInputs.zipWithIndex.map {
          case (child, childOrdinal) =>
            val physicalChild = child.asInstanceOf[StreamPhysicalRel]
            val supportOnlyAfter = join.inputUniqueKeyContainsJoinKey(childOrdinal)
            val inputModifyKindSet = getModifyKindSet(physicalChild)
            if (onlyAfterByParent) {
              if (inputModifyKindSet.contains(ModifyKind.UPDATE) && !supportOnlyAfter) {
                // the parent requires only-after, however, the join doesn't support this
                None
              } else {
                this.visit(physicalChild, onlyAfterOrNone(inputModifyKindSet))
              }
            } else {
              this.visit(physicalChild, beforeAfterOrNone(inputModifyKindSet))
            }
        }
        if (children.exists(_.isEmpty)) {
          None
        } else {
          createNewNode(join, Some(children.flatten.toList), requiredTrait)
        }

      case temporalJoin: StreamPhysicalTemporalJoin =>
        val left = temporalJoin.getLeft.asInstanceOf[StreamPhysicalRel]
        val right = temporalJoin.getRight.asInstanceOf[StreamPhysicalRel]

        // the left input required trait depends on it's parent in temporal join
        // the left input will send message to parent
        val requiredUpdateBeforeByParent = requiredTrait.updateKind == UpdateKind.BEFORE_AND_AFTER
        val leftInputModifyKindSet = getModifyKindSet(left)
        val leftRequiredTrait = if (requiredUpdateBeforeByParent) {
          beforeAfterOrNone(leftInputModifyKindSet)
        } else {
          onlyAfterOrNone(leftInputModifyKindSet)
        }
        val newLeftOption = this.visit(left, leftRequiredTrait)

        val rightInputModifyKindSet = getModifyKindSet(right)
        // currently temporal join support changelog stream as the right side
        // so it supports both ONLY_AFTER and BEFORE_AFTER, but prefer ONLY_AFTER
        val newRightOption = this.visit(right, onlyAfterOrNone(rightInputModifyKindSet)) match {
          case Some(newRight) => Some(newRight)
          case None =>
            val beforeAfter = beforeAfterOrNone(rightInputModifyKindSet)
            this.visit(right, beforeAfter)
        }

        (newLeftOption, newRightOption) match {
          case (Some(newLeft), Some(newRight)) =>
            val leftTrait = newLeft.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
            createNewNode(temporalJoin, Some(List(newLeft, newRight)), leftTrait)
          case _ =>
            None
        }

      case calc: StreamPhysicalCalcBase =>
        if (requiredTrait == UpdateKindTrait.ONLY_UPDATE_AFTER &&
            calc.getProgram.getCondition != null) {
          // we don't expect filter to satisfy ONLY_UPDATE_AFTER update kind,
          // to solve the bad case like a single 'cnt < 10' condition after aggregation.
          // See FLINK-9528.
          None
        } else {
          // otherwise, forward UpdateKind requirement
          visitChildren(rel, requiredTrait) match {
            case None => None
            case Some(children) =>
              val childTrait = children.head.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
              createNewNode(rel, Some(children), childTrait)
          }
        }

      case _: StreamPhysicalCorrelateBase | _: StreamPhysicalLookupJoin |
           _: StreamPhysicalExchange | _: StreamPhysicalExpand |
           _: StreamPhysicalMiniBatchAssigner | _: StreamPhysicalWatermarkAssigner |
           _: StreamPhysicalWindowTableFunction =>
        // transparent forward requiredTrait to children
        visitChildren(rel, requiredTrait) match {
          case None => None
          case Some(children) =>
            val childTrait = children.head.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
            createNewNode(rel, Some(children), childTrait)
        }

      case union: StreamPhysicalUnion =>
        val children = union.getInputs.map {
          case child: StreamPhysicalRel =>
            val childModifyKindSet = getModifyKindSet(child)
            val requiredChildTrait = if (childModifyKindSet.isInsertOnly) {
              UpdateKindTrait.NONE
            } else {
              requiredTrait
            }
            this.visit(child, requiredChildTrait)
        }.toList

        if (children.exists(_.isEmpty)) {
          None
        } else {
          val updateKinds = children.flatten
            .map(_.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE))
          // union can just forward changes, can't actively satisfy to another changelog mode
          val providedTrait = if (updateKinds.forall(k => UpdateKindTrait.NONE == k)) {
            // if all the children is NO_UPDATE, union is NO_UPDATE
            UpdateKindTrait.NONE
          } else {
            // otherwise, merge update kinds.
            val merged = updateKinds
              .map(_.updateKind)
              .reduce { (l, r) =>
                (l, r) match {
                  case (UpdateKind.NONE, r: UpdateKind) => r
                  case (l: UpdateKind, UpdateKind.NONE) => l
                  case (l: UpdateKind, r: UpdateKind) if l == r => l
                  // UNION doesn't support to union ONLY_UPDATE_AFTER and BEFORE_AND_AFTER inputs
                  case (_, _) => return None
                }
              }
            new UpdateKindTrait(merged)
          }
          createNewNode(union, Some(children.flatten), providedTrait)
        }

      case normalize: StreamPhysicalChangelogNormalize =>
        val contextResolvedTable = normalize.contextResolvedTable
        val tableIdentifier = contextResolvedTable.getIdentifier
        if (!contextResolvedTable.isAnonymous
          && requiredTrait == UpdateKindTrait.ONLY_UPDATE_AFTER) {
          val catalogName = tableIdentifier.getCatalogName
          val catalog = context.getCatalogManager.getCatalog(catalogName).orElse(null)
          val catalogTable = contextResolvedTable.getResolvedTable[ResolvedCatalogBaseTable[_]]
          if (ManagedTableListener.isManagedTable(catalog, catalogTable)) {
            // if requiredTrait is ONLY_UPDATE_AFTER and table is ManagedTable,
            // we can eliminate current normalize stage,
            // cuz ManagedTable has preserved complete delete messages.
            val input = normalize.getInput match {
              case exchange: StreamPhysicalExchange =>
                exchange.getInput
              case _ =>
                normalize.getInput
            }
            val inputPhysicalRel = input.asInstanceOf[StreamPhysicalRel]
            return this.visit(inputPhysicalRel, UpdateKindTrait.ONLY_UPDATE_AFTER)
          }
        }
        // changelog normalize currently only supports input only sending UPDATE_AFTER
        val children = visitChildren(normalize, UpdateKindTrait.ONLY_UPDATE_AFTER)
        // use requiredTrait as providedTrait,
        // because changelog normalize supports all kinds of UpdateKind
        createNewNode(rel, children, requiredTrait)

      case ts: StreamPhysicalTableSourceScan =>
        // currently only support BEFORE_AND_AFTER if source produces updates
        val providedTrait = UpdateKindTrait.fromChangelogMode(ts.tableSource.getChangelogMode)
        val newSource = createNewNode(rel, Some(List()), providedTrait)
        if (providedTrait.equals(UpdateKindTrait.BEFORE_AND_AFTER) &&
            requiredTrait.equals(UpdateKindTrait.ONLY_UPDATE_AFTER)) {
          // requiring only-after, but the source is CDC source, then drop update_before manually
          val dropUB = new StreamPhysicalDropUpdateBefore(rel.getCluster, rel.getTraitSet, rel)
          createNewNode(dropUB, newSource.map(s => List(s)), requiredTrait)
        } else {
          newSource
        }

      case _: StreamPhysicalDataStreamScan | _: StreamPhysicalLegacyTableSourceScan |
           _: StreamPhysicalValues =>
        createNewNode(rel, Some(List()), UpdateKindTrait.NONE)

      case scan: StreamPhysicalIntermediateTableScan =>
        val providedTrait = if (scan.intermediateTable.isUpdateBeforeRequired) {
          // we can't drop UPDATE_BEFORE if it is required by other parent blocks
          UpdateKindTrait.BEFORE_AND_AFTER
        } else {
          requiredTrait
        }
        if (!providedTrait.satisfies(requiredTrait)) {
          // require ONLY_AFTER but can only provide BEFORE_AND_AFTER
          None
        } else {
          createNewNode(rel, Some(List()), providedTrait)
        }

      case _ =>
        throw new UnsupportedOperationException(
          s"Unsupported visit for ${rel.getClass.getSimpleName}")

    }

    private def visitChildren(
        parent: StreamPhysicalRel,
        requiredChildrenTrait: UpdateKindTrait): Option[List[StreamPhysicalRel]] = {
      val newChildren = for (child <- parent.getInputs) yield {
        this.visit(child.asInstanceOf[StreamPhysicalRel], requiredChildrenTrait) match {
          case None =>
            // return None if one of the children can't satisfy
            return None
          case Some(newChild) =>
            val providedTrait = newChild.getTraitSet.getTrait(UpdateKindTraitDef.INSTANCE)
            if (!providedTrait.satisfies(requiredChildrenTrait)) {
              // the provided trait can't satisfy required trait, thus we should return None.
              return None
            }
            newChild
        }
      }
      Some(newChildren.toList)
    }

    private def createNewNode(
        node: StreamPhysicalRel,
        childrenOption: Option[List[StreamPhysicalRel]],
        providedTrait: UpdateKindTrait): Option[StreamPhysicalRel] = childrenOption match {
      case None =>
        None
      case Some(children) =>
        val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
        val nodeDescription = node.getRelDetailedDescription
        val isUpdateKindValid = providedTrait.updateKind match {
          case UpdateKind.NONE =>
            !modifyKindSetTrait.modifyKindSet.contains(ModifyKind.UPDATE)
          case UpdateKind.BEFORE_AND_AFTER | UpdateKind.ONLY_UPDATE_AFTER =>
            modifyKindSetTrait.modifyKindSet.contains(ModifyKind.UPDATE)
        }
        if (!isUpdateKindValid) {
          throw new TableException(s"UpdateKindTrait $providedTrait conflicts with " +
            s"ModifyKindSetTrait $modifyKindSetTrait. " +
            s"This is a bug in planner, please file an issue. \n" +
            s"Current node is $nodeDescription.")
        }
        val newTraitSet = node.getTraitSet.plus(providedTrait)
        Some(node.copy(newTraitSet, children).asInstanceOf[StreamPhysicalRel])
    }

    /**
     * Try all possible rank strategies and return the first viable new node.
     * @param rankStrategies all possible supported rank strategy by current node
     * @param requiredUpdateKindTrait the required UpdateKindTrait by parent of rank node
     * @param applyRankStrategy a function to apply rank strategy to get a new copied rank node
     */
    private def visitRankStrategies(
        rankStrategies: Seq[RankProcessStrategy],
        requiredUpdateKindTrait: UpdateKindTrait,
        applyRankStrategy: RankProcessStrategy => StreamPhysicalRel): Option[StreamPhysicalRel] = {
      // go pass every RankProcessStrategy, apply the rank strategy to get a new copied rank node,
      // return the first satisfied converted node
      for (strategy <- rankStrategies) {
        val requiredChildrenTrait = strategy match {
          case _: UpdateFastStrategy => UpdateKindTrait.ONLY_UPDATE_AFTER
          case _: RetractStrategy => UpdateKindTrait.BEFORE_AND_AFTER
          case _: AppendFastStrategy => UpdateKindTrait.NONE
        }
        val node = applyRankStrategy(strategy)
        val children = visitChildren(node, requiredChildrenTrait)
        val newNode = createNewNode(node, children, requiredUpdateKindTrait)
        if (newNode.isDefined) {
          return newNode
        }
      }
      None
    }

    private def visitSink(
        sink: StreamPhysicalRel,
        sinkRequiredTraits: Seq[UpdateKindTrait]): Option[StreamPhysicalRel] = {
      val children = sinkRequiredTraits.flatMap(t => visitChildren(sink, t))
      if (children.isEmpty) {
        None
      } else {
        val sinkTrait = sink.getTraitSet.plus(UpdateKindTrait.NONE)
        Some(sink.copy(sinkTrait, children.head).asInstanceOf[StreamPhysicalRel])
      }
    }

    /**
     * Infer sink required traits by the sink node and its input. Sink required traits is based on
     * the sink node's changelog mode, the only exception is when sink's pk(s) not exactly the same
     * as the changeLogUpsertKeys and sink' changelog mode is ONLY_UPDATE_AFTER.
     */
    private def inferSinkRequiredTraits(sink: StreamPhysicalSink): Seq[UpdateKindTrait] = {
      val childModifyKindSet = getModifyKindSet(sink.getInput)
      val onlyAfter = onlyAfterOrNone(childModifyKindSet)
      val beforeAndAfter = beforeAfterOrNone(childModifyKindSet)
      val sinkTrait = UpdateKindTrait.fromChangelogMode(
        sink.tableSink.getChangelogMode(childModifyKindSet.toChangelogMode))

      val sinkRequiredTraits = if (sinkTrait.equals(ONLY_UPDATE_AFTER)) {
        // if sink's pk(s) are not exactly match input changeLogUpsertKeys then it will fallback
        // to beforeAndAfter mode for the correctness
        var requireBeforeAndAfter: Boolean = false
        val sinkDefinedPks = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes

        if (sinkDefinedPks.nonEmpty) {
          val sinkPks = ImmutableBitSet.of(sinkDefinedPks: _*)
          val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
          val changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput)
          // if input is UA only, primary key != upsert key (upsert key can be null) we should
          // fallback to beforeAndAfter.
          // Notice: even sink pk(s) contains input upsert key we cannot optimize to UA only,
          // this differs from batch job's unique key inference
          if (changeLogUpsertKeys == null || !changeLogUpsertKeys.exists {_.equals(sinkPks)}) {
            requireBeforeAndAfter = true
          }
        }
        if (requireBeforeAndAfter) {
          Seq(beforeAndAfter)
        } else {
          Seq(onlyAfter, beforeAndAfter)
        }
      } else if (sinkTrait.equals(BEFORE_AND_AFTER)){
        Seq(beforeAndAfter)
      } else {
        Seq(UpdateKindTrait.NONE)
      }
      sinkRequiredTraits
    }

    /**
     *  Analyze whether to enable upsertMaterialize or not. In these case will return true:
     *  1. when `TABLE_EXEC_SINK_UPSERT_MATERIALIZE` set to FORCE and sink's primary key nonempty.
     *  2. when `TABLE_EXEC_SINK_UPSERT_MATERIALIZE` set to AUTO and sink's primary key doesn't
     *  contain upsertKeys of the input update stream.
     */
    private def analyzeUpsertMaterializeStrategy(sink: StreamPhysicalSink): Boolean = {
      val tableConfig = unwrapTableConfig(sink)
      val inputChangelogMode = ChangelogPlanUtils.getChangelogMode(
        sink.getInput.asInstanceOf[StreamPhysicalRel]).get
      val primaryKeys = sink.contextResolvedTable.getResolvedSchema.getPrimaryKeyIndexes
      val upsertMaterialize = tableConfig.get(
        ExecutionConfigOptions.TABLE_EXEC_SINK_UPSERT_MATERIALIZE) match {
        case UpsertMaterialize.FORCE => primaryKeys.nonEmpty
        case UpsertMaterialize.NONE => false
        case UpsertMaterialize.AUTO =>
          val sinkAcceptInsertOnly = sink.tableSink.getChangelogMode(inputChangelogMode)
              .containsOnly(RowKind.INSERT)
          val inputInsertOnly = inputChangelogMode.containsOnly(RowKind.INSERT)

          if (!sinkAcceptInsertOnly && !inputInsertOnly && primaryKeys.nonEmpty) {
            val pks = ImmutableBitSet.of(primaryKeys: _*)
            val fmq = FlinkRelMetadataQuery.reuseOrCreate(sink.getCluster.getMetadataQuery)
            val changeLogUpsertKeys = fmq.getUpsertKeys(sink.getInput)
            // if input has update and primary key != upsert key (upsert key can be null) we should
            // enable upsertMaterialize. An optimize is: do not enable upsertMaterialize when sink
            // pk(s) contains input changeLogUpsertKeys
            if (changeLogUpsertKeys == null || !changeLogUpsertKeys.exists(pks.contains)) {
              true
            } else {
              false
            }
          } else {
            false
          }
      }
      upsertMaterialize
    }
  }

  // -------------------------------------------------------------------------------------------

  private def getModifyKindSet(node: RelNode): ModifyKindSet = {
    val modifyKindSetTrait = node.getTraitSet.getTrait(ModifyKindSetTraitDef.INSTANCE)
    modifyKindSetTrait.modifyKindSet
  }
}
