/*
* Copyright (C) 2016 Kyligence Inc. All rights reserved.
*
* http://kyligence.io
*
* This software is the confidential and proprietary information of
* Kyligence Inc. ("Confidential Information"). You shall not disclose
* such Confidential Information and shall use it only in accordance
* with the terms of the license agreement you entered into with
* Kyligence Inc.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
* "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
* LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
* A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
* OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
* SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
* LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
* DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
* THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
* (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

package org.apache.kylin.engine.spark.planner

import java.io.IOException
import java.util.Collections
import java.{lang, util}

import org.apache.kylin.engine.spark.job.NSparkCubingUtil
import org.apache.kylin.engine.spark.job.step.build.FlatTableStage
import org.apache.kylin.metadata.cube.model.NDataSegment
import org.apache.kylin.metadata.model.{JoinTableDesc, PartitionDesc, SegmentRange, TableRef}
import org.apache.kylin.planner.internal.PlannerConf
import org.apache.kylin.planner.plans.CubePlan
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConverters._

case class ProbabilisticCounting(spark: SparkSession, plan: CubePlan) extends Logging {

  private val conf = PlannerConf.get()

  private lazy val estimator = hllEstimator

  def countMap(): util.Map[util.BitSet, lang.Long] = estimator.countMap()

  @throws(classOf[IOException])
  private def hllEstimator: HLLCountEstimator = {
    val factTableRef = plan.getModel.getRootFactTable
    val joinTableSeq = plan.getModel.getJoinTables.asScala
    val partitionDesc = plan.getModel.getPartitionDesc
    // wip: time window should be configurable considering there's no segment
    val fakeSegment = new NDataSegment(plan.getDataflow, //
      new SegmentRange.TimePartitionedSegmentRange(conf.dataRangeStart(), conf.dataRangeEnd()))

    // cuboid bits -> columns
    val aggColumnMap = plan.getCuboidColumnMap.asScala.toMap
    // cuboid columns in flat table
    val aggColumnSeq = plan.getColumnList(
      aggColumnMap.keys // wip: cuboid bits should be immutable here
        .foldLeft(plan.emptyBits)((bits, cuboidBits) => {
          bits.or(cuboidBits)
          bits
        })).asScala
    // alias -> index in flat table
    val aliasIndexMap = aggColumnSeq.map(_.getAliasDotName).zipWithIndex.toMap

    // cuboid bits -> column indexes in flat table
    val cuboidIndexMap = aggColumnMap.map(t => (t._1, // cuboid bits
      t._2.asScala.map(_.getAliasDotName).map(aliasIndexMap) // alias -> index
        .map(Integer.valueOf).asJava))

    // selected columns
    val columnSeq = aggColumnSeq.map(_.getBackTickIdentity).map(NSparkCubingUtil.convertFromDotWithBackTick)
    // effective columns
    val flatTable = createFlatTable(factTableRef, joinTableSeq, partitionDesc, fakeSegment)

    new HLLCountEstimator(flatTable.selectExpr(columnSeq: _*), // essential dimension column only
      // calculate the tuple count of cuboid and the row count of flat table in a single traversal
      (cuboidIndexMap ++ Map(plan.ceilingBits() -> Collections.emptyList[Integer])).asJava)
  }


  private def createFlatTable(factTableRef: TableRef, joinTableSeq: Seq[JoinTableDesc],
                              partitionDesc: PartitionDesc, segment: NDataSegment): Dataset[Row] = {
    // 1. apply cc column expr;
    // 2. apply time partition;
    // wip: 3. apply model filter condition;
    // wip: 4. apply infer filter condition.

    val factTable = FlatTableStage.createTable(factTableRef)(spark)
    // time range fact table
    val partialFactTable = appendTimeRange(factTable, partitionDesc, segment)

    val ccSet = factTableRef.getColumns.asScala.filter(_.getColumnDesc.isComputedColumn).toSet
    // cc fact table
    val ccFactTable = FlatTableStage.fulfillDS(partialFactTable, ccSet, factTableRef)

    // flat table
    val joinTableMap = FlatTableStage.createJoinTableMap(joinTableSeq, t => FlatTableStage.createTable(t)(spark))
    val flatTable = joinTableMap.foldLeft(ccFactTable)(
      (jointDataset: Dataset[Row], t: (JoinTableDesc, Dataset[Row])) =>
        FlatTableStage.joinTableDataset(factTableRef.getTableDesc, t._1, jointDataset, t._2))

    // cc flat table
    FlatTableStage.concatCCs(flatTable, ccSet)
  }

  private def appendTimeRange(factTable: Dataset[Row],
                              partitionDesc: PartitionDesc, segment: NDataSegment): Dataset[Row] =
    if (PartitionDesc.isEmptyPartitionDesc(partitionDesc)
      || segment == null || segment.getSegRange == null || segment.getSegRange.isInfinite) {
      // wip: to be configurable, avoid large flat table traversing
      factTable
    } else {
      val cond = partitionDesc.getPartitionConditionBuilder
        .buildDateRangeCondition(partitionDesc, segment, segment.getSegRange)
      logInfo(s"Append time range: $cond")
      factTable.where(cond)
    }

}
