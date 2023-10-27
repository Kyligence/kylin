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
package org.apache.kylin.query.runtime

import java.util

import org.apache.calcite.DataContext
import org.apache.calcite.rel.{RelNode, RelVisitor}
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.engine.spark.utils.LogEx
import org.apache.kylin.query.relnode._
import org.apache.kylin.query.runtime.plan._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.{DataFrame, SparderEnv, SparkInternalAgent}

class CalciteToSparkPlaner(dataContext: DataContext) extends RelVisitor with LogEx {
  private val stack = new util.ArrayDeque[LogicalPlan]()
  private val setOpStack = new util.ArrayDeque[Int]()
  private var unionLayer = 0

  // clear cache before any op
  cleanCache()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[OlapUnionRel]) {
      unionLayer = unionLayer + 1
    }
    if (node.isInstanceOf[OlapUnionRel] || node.isInstanceOf[OlapMinusRel]) {
      setOpStack.offerLast(stack.size())
    }
    // skip non runtime joins children
    // cases to skip children visit
    // 1. current node is a OlapJoinRel and is not a runtime join
    // 2. current node is a OlapNonEquiJoinRel and is not a runtime join
    if (!(node.isInstanceOf[OlapJoinRel] && !node.asInstanceOf[OlapJoinRel].isRuntimeJoin) &&
      !(node.isInstanceOf[OlapNonEquiJoinRel] && !node.asInstanceOf[OlapNonEquiJoinRel].isRuntimeJoin)) {
      node.childrenAccept(this)
    }
    stack.offerLast(node match {
      case rel: OlapTableScan => convertTableScan(rel)
      case rel: OlapFilterRel =>
        logTime("filter") {
          FilterPlan.filter(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapProjectRel =>
        logTime("project") {
          ProjectPlan.select(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapLimitRel =>
        logTime("limit") {
          LimitPlan.limit(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapSortRel =>
        logTime("sort") {
          SortPlan.sort(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapWindowRel =>
        logTime("window") {
          WindowPlan.window(stack.pollLast(), rel, dataContext)
        }
      case rel: OlapAggregateRel =>
        logTime("agg") {
          AggregatePlan.agg(stack.pollLast(), rel)
        }
      case rel: OlapJoinRel => convertJoinRel(rel)
      case rel: OlapNonEquiJoinRel => convertNonEquiJoinRel(rel)
      case rel: OlapUnionRel =>
        val size = setOpStack.pollLast()
        var unionBlocks = Range(0, stack.size() - size).map(_ => stack.pollLast())
        if (KylinConfig.getInstanceFromEnv.isCollectUnionInOrder) {
          unionBlocks = unionBlocks.reverse
        }
        logTime("union") {
          plan.UnionPlan.union(unionBlocks, rel, dataContext)
        }
      case rel: OlapMinusRel =>
        val size = setOpStack.pollLast()
        logTime("minus") {
          plan.MinusPlan.minus(Range(0, stack.size() - size).map(_ => stack.pollLast()).reverse, rel, dataContext)
        }
      case rel: OlapValuesRel =>
        logTime("values") {
          ValuesPlan.values(rel)
        }
      case rel: OlapModelViewRel =>
        logTime("modelview") {
          stack.pollLast()
        }
    })
    if (node.isInstanceOf[OlapUnionRel]) {
      unionLayer = unionLayer - 1
    }
  }

  private def convertTableScan(rel: OlapTableScan): LogicalPlan = {
    rel.getContext.genExecFunc(rel, rel.getTableName) match {
      case "executeLookupTableQuery" =>
        logTime("createLookupTable") {
          TableScanPlan.createLookupTable(rel)
        }
      case "executeOlapQuery" =>
        logTime("createOlapTable") {
          TableScanPlan.createOlapTable(rel)
        }
      case "executeSimpleAggregationQuery" =>
        logTime("createSingleRow") {
          TableScanPlan.createSingleRow()
        }
      case "executeMetadataQuery" =>
        logTime("createMetadataTable") {
          TableScanPlan.createMetadataTable(rel)
        }
    }
  }

  private def convertJoinRel(rel: OlapJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      rel.getContext.genExecFunc(rel, "") match {
        case "executeMetadataQuery" =>
          logTime("createMetadataTable") {
            TableScanPlan.createMetadataTable(rel)
          }
        case _ =>
          logTime("join with table scan") {
            TableScanPlan.createOlapTable(rel)
          }
      }
    } else {
      val right = stack.pollLast()
      val left = stack.pollLast()
      logTime("join") {
        plan.JoinPlan.join(Seq.apply(left, right), rel)
      }
    }
  }

  private def convertNonEquiJoinRel(rel: OlapNonEquiJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      logTime("join with table scan") {
        TableScanPlan.createOlapTable(rel)
      }
    } else {
      val right = stack.pollLast()
      val left = stack.pollLast()
      logTime("non-equi join") {
        plan.JoinPlan.nonEquiJoin(Seq.apply(left, right), rel, dataContext)
      }
    }
  }

  def cleanCache(): Unit = {
    TableScanPlan.cachePlan.get().clear()
  }

  def getResult(): DataFrame = {
    val logicalPlan = stack.pollLast()
    SparkInternalAgent.getDataFrame(SparderEnv.getSparkSession, logicalPlan)
  }
}
