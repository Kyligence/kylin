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
  private val stack = new util.Stack[LogicalPlan]()
  private val setOpStack = new util.Stack[Int]()
  private var unionLayer = 0

  // clear cache before any op
  cleanCache()

  override def visit(node: RelNode, ordinal: Int, parent: RelNode): Unit = {
    if (node.isInstanceOf[KapUnionRel]) {
      unionLayer = unionLayer + 1
    }
    if (node.isInstanceOf[KapUnionRel] || node.isInstanceOf[KapMinusRel]) {
      setOpStack.push(stack.size())
    }
    // skip non runtime joins children
    // cases to skip children visit
    // 1. current node is a KapJoinRel and is not a runtime join
    // 2. current node is a KapNonEquiJoinRel and is not a runtime join
    if (!(node.isInstanceOf[KapJoinRel] && !node.asInstanceOf[KapJoinRel].isRuntimeJoin) &&
      !(node.isInstanceOf[KapNonEquiJoinRel] && !node.asInstanceOf[KapNonEquiJoinRel].isRuntimeJoin)) {
      node.childrenAccept(this)
    }
    stack.push(node match {
      case rel: KapTableScan => convertTableScan(rel)
      case rel: KapFilterRel =>
        logTime("filter") {
          FilterPlan.filter(stack.pop(), rel, dataContext)
        }
      case rel: KapProjectRel =>
        logTime("project") {
          ProjectPlan.select(stack.pop(), rel, dataContext)
        }
      case rel: KapLimitRel =>
        logTime("limit") {
          LimitPlan.limit(stack.pop(), rel, dataContext)
        }
      case rel: KapSortRel =>
        logTime("sort") {
          SortPlan.sort(stack.pop(), rel, dataContext)
        }
      case rel: KapWindowRel =>
        logTime("window") {
          WindowPlan.window(stack.pop(), rel, dataContext)
        }
      case rel: KapAggregateRel =>
        logTime("agg") {
          AggregatePlan.agg(stack.pop(), rel)
        }
      case rel: KapJoinRel => convertJoinRel(rel)
      case rel: KapNonEquiJoinRel => convertNonEquiJoinRel(rel)
      case rel: KapUnionRel =>
        val size = setOpStack.pop()
        var unionBlocks = Range(0, stack.size() - size).map(a => stack.pop())
        if (KylinConfig.getInstanceFromEnv.isCollectUnionInOrder) {
          unionBlocks = unionBlocks.reverse
        }
        logTime("union") {
          plan.UnionPlan.union(unionBlocks, rel, dataContext)
        }
      case rel: KapMinusRel =>
        val size = setOpStack.pop()
        logTime("minus") {
          plan.MinusPlan.minus(Range(0, stack.size() - size).map(a => stack.pop()).reverse, rel, dataContext)
        }
      case rel: KapValuesRel =>
        logTime("values") {
          ValuesPlan.values(rel)
        }
      case rel: KapModelViewRel =>
        logTime("modelview") {
          stack.pop()
        }
    })
    if (node.isInstanceOf[KapUnionRel]) {
      unionLayer = unionLayer - 1
    }
  }

  private def convertTableScan(rel: KapTableScan): LogicalPlan = {
    rel.getContext.genExecFunc(rel, rel.getTableName) match {
      case "executeLookupTableQuery" =>
        logTime("createLookupTable") {
          TableScanPlan.createLookupTable(rel)
        }
      case "executeOLAPQuery" =>
        logTime("createOLAPTable") {
          TableScanPlan.createOLAPTable(rel)
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

  private def convertJoinRel(rel: KapJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      rel.getContext.genExecFunc(rel, "") match {
        case "executeMetadataQuery" =>
          logTime("createMetadataTable") {
            TableScanPlan.createMetadataTable(rel)
          }
        case _ =>
          logTime("join with table scan") {
            TableScanPlan.createOLAPTable(rel)
          }
      }
    } else {
      val right = stack.pop()
      val left = stack.pop()
      logTime("join") {
        plan.JoinPlan.join(Seq.apply(left, right), rel)
      }
    }
  }

  private def convertNonEquiJoinRel(rel: KapNonEquiJoinRel): LogicalPlan = {
    if (!rel.isRuntimeJoin) {
      logTime("join with table scan") {
        TableScanPlan.createOLAPTable(rel)
      }
    } else {
      val right = stack.pop()
      val left = stack.pop()
      logTime("non-equi join") {
        plan.JoinPlan.nonEquiJoin(Seq.apply(left, right), rel, dataContext)
      }
    }
  }

  def cleanCache(): Unit = {
    TableScanPlan.cachePlan.get().clear()
  }

  def getResult(): DataFrame = {
    val logicalPlan = stack.pop()
    SparkInternalAgent.getDataFrame(SparderEnv.getSparkSession, logicalPlan)
  }
}
