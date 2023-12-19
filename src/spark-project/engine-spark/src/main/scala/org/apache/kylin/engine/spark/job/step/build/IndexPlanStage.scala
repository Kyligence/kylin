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

package org.apache.kylin.engine.spark.job.step.build

import org.apache.kylin.engine.spark.application.SparkApplication
import org.apache.kylin.engine.spark.job.IndexPlanOptimizeJob
import org.apache.kylin.engine.spark.job.step.StageExec
import org.apache.kylin.engine.spark.planner.{BeneficialCuboidSelection, QueryStatsExtraction}
import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.kylin.planner.CubePlanner
import org.apache.kylin.planner.candy.PlanTransformer
import org.apache.kylin.planner.plans.CubePlan
import org.apache.kylin.planner.rules.RuleBatch
import org.apache.kylin.planner.util.Utils

import scala.collection.JavaConverters._

class IndexPlanStage(ctx: IndexPlanOptimizeJob) extends StageExec {

  override def getStageName: String = "IndexPlanStage"

  override def getJobContext: SparkApplication = ctx

  override def execute(): Unit = {
    val spark = ctx.getSparkSession
    val planner = CubePlanner.builder() //
      .config(ctx.getConfig) //
      .model(ctx.getModel) //
      .indexPlan(ctx.getIndexPlan) //
      .dataflow(ctx.getDataflow) //
      .params(ctx.getParams).build()

    val projectDir = ctx.getConfig.getHdfsWorkingDirectory(ctx.getProject)
    // wip: to be configurable
    val statsDaySet = Utils.recentOneMonthDays()
    // offer additional rule to the head
    val optimizer = planner.plannerState.getOptimizer
    optimizer.offerHead(new RuleBatch[CubePlan]("Query stats cuboid set extraction",
      Lists.newArrayList(QueryStatsExtraction(spark, projectDir, statsDaySet.asScala.toSet))))

    // offer additional rule to the tail
    optimizer.offerTail(new RuleBatch[CubePlan]("Cost base cuboid set selection",
      Lists.newArrayList(BeneficialCuboidSelection(spark))))

    logInfo(s"Before optimization: ${planner.plan.treeString}")
    val plan = planner.optimizeCube()
    logInfo(s"After optimization: ${plan.treeString}")
    // transform to recommendation items
    val trans = new PlanTransformer(plan, ctx.getConfig)
    trans.transformAndSave()
  }

}
