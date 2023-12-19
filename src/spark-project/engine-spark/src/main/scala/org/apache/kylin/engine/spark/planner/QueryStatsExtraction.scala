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

import java.{lang, util}

import org.apache.hadoop.fs.Path
import org.apache.kylin.guava30.shaded.common.collect.Maps
import org.apache.kylin.planner.plans.CubePlan
import org.apache.kylin.planner.rules.Rule
import org.apache.kylin.planner.stats.RawRecInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.execution.datasources.json.{JsonFileFormat, QueryRecHistorySchema}
import org.apache.spark.sql.execution.datasources.{FileStatusCache, HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.JavaConverters._

case class QueryStatsExtraction(spark: SparkSession,
                                projectDir: String,
                                statsDaySet: Set[String]) extends Rule[CubePlan] with Logging {

  private val REC_INFO_TEMP_VIEW = "__RecInfoTempView"
  private val REC_INFO_AGGREGATE_SQL = //
    "select recId, count(1) as recTimes, sum(cpuTime) as totalCpuTime, first(recInfo, true) as recInfo" +
      s" from $REC_INFO_TEMP_VIEW group by recId order by totalCpuTime desc"

  override def apply(plan: CubePlan): CubePlan = {
    val iter = collectRecInfoAsIterator(plan.getModel.getId, plan.getModel.getSemanticVersion)

    val freqMap = Maps.newHashMap[util.BitSet, lang.Long]()
    // add cuboid to plan
    while (iter.hasNext) {
      val rawRecInfo = iter.next()
      if (rawRecInfo != null) {
        val recInfo = rawRecInfo.getRecInfo
        val columnBits = plan.getColumnBits(recInfo.getColumns)
        val dimensionBits = plan.getColumnBits(recInfo.getDimensions)
        val measureBits = plan.getColumnBits(recInfo.getMeasures.asScala
          .flatMap(_.getFunction.getParameters.asScala
            .filter(_.isColumnType).map(_.getValue)).asJava)
        // wip: cc expr as measure parameter
        columnBits.andNot(measureBits)
        dimensionBits.or(columnBits)
        // add cuboid or merge measures
        plan.addMergeSetHit(dimensionBits, recInfo.getMeasures)
        // merge frequency stats
        freqMap.merge(dimensionBits, rawRecInfo.getRecTimes, (a, b) => a + b)
      }
    }
    // report stats
    plan.reportQueryFrequency(freqMap)
    // result plan
    plan
  }

  def collectRecInfoAsIterator(modelId: String, semanticVersion: Int): util.Iterator[RawRecInfo] = {
    val fsRelation = createFsRelation(projectDir, statsDaySet)
    val flatRecInfoSchema: StructType = QueryRecHistorySchema.flatRecInfoSchema2()
    val recInfoDataset = spark.baseRelationToDataFrame(fsRelation) //
      .filter("recDetailMap is not null") //
      .flatMap { input => //
        val cpuTime = input.getAs[Long]("cpuTime")
        input.getAs[Map[String, Row]]("recDetailMap").values //
          .filter(r => modelId.equals(r.getAs[String]("modelId"))) //
          .filter(r => semanticVersion.equals(r.getAs[Int]("semanticVersion"))) //
          .flatMap { r => //
            r.getAs[Seq[Row]]("layoutRecs").map(recInfo => //
              Row(recInfo.getAs[String]("uniqueId"), cpuTime, recInfo))
          }
      }(RowEncoder.apply(flatRecInfoSchema))

    // create temp view
    recInfoDataset.createOrReplaceTempView(REC_INFO_TEMP_VIEW)

    // convert to iterator
    val recInfoIter = spark.sql(REC_INFO_AGGREGATE_SQL).toJSON.toLocalIterator

    new util.Iterator[RawRecInfo] {
      override def hasNext: Boolean = recInfoIter.hasNext

      override def next(): RawRecInfo = RawRecInfo.parse(recInfoIter.next())
    }
  }

  private def createFsRelation(baseDir: String, daySet: Set[String]): HadoopFsRelation = {
    // path: {working_dir}/{project}/query_rec/history/{yyyy-MM-dd}/
    // fileName: query_history_{timestamp}_{randomId}.json.snappy
    val historyPath = new Path(baseDir, "query_rec/history")
    val fs = historyPath.getFileSystem(spark.sparkContext.hadoopConfiguration)
    if (!fs.exists(historyPath)) {
      fs.mkdirs(historyPath)
    }

    val filePaths = fs.listStatus(historyPath, (p: Path) => daySet.contains(p.getName)).map(_.getPath)
    val fileCache: FileStatusCache = FileStatusCache.getOrCreate(spark)
    val fileIndex = new InMemoryFileIndex(spark, filePaths, Map.empty, None, fileCache)
    val schema: StructType = QueryRecHistorySchema.queryRecHistorySchema()
    // fs relation based on specified file paths
    HadoopFsRelation(fileIndex, //
      StructType(Seq.empty[StructField]), schema, None, new JsonFileFormat(), Map.empty)(spark)
  }

}
