/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 * http://kyligence.io
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
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
 *
 */
package org.apache.spark.sql.execution

import java.util.concurrent.TimeUnit

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.common.LocalMetadata
import org.apache.spark.sql.delta.KylinDeltaLogFileIndex
import org.apache.spark.sql.delta.actions.AddFile
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasource.{FilePruner, KylinDeltaSourceStrategy, KylinSourceStrategy, LayoutFileSourceStrategy}
import org.apache.spark.sql.execution.datasources.{CacheFileScanRDD, FileIndex, FileScanRDD, HadoopFsRelation, LogicalRelation, PartitionDirectory}
import org.mockito.{ArgumentMatchers, Mockito}

import com.google.common.cache.CacheBuilder

import io.kyligence.kap.cache.softaffinity.SoftAffinityConstants

class KylinFileSourceScanExecSuite extends SparkFunSuite
  with SQLHelper with AdaptiveSparkPlanHelper with LocalMetadata {

  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  test("Create sharding read RDD with Soft affinity - CacheFileScanRDD") {
    SparkSession.cleanupAnyExistingSession()
    val spark = SparkSession.builder()
      .master("local[1]")
      .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
      .withExtensions { ext =>
        ext.injectPlannerStrategy(_ => KylinSourceStrategy)
        ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        ext.injectPlannerStrategy(_ => KylinDeltaSourceStrategy)
      }
      .getOrCreate()

    withTempPath { path =>
      val tempDir = path.getCanonicalPath

      val df = createSimpleFilePrunnerDF(spark, tempDir)
      assert(getFileSourceScanExec(df).isInstanceOf[KylinFileSourceScanExec])
      assert(getFileSourceScanExec(df).asInstanceOf[KylinFileSourceScanExec].inputRDD.isInstanceOf[CacheFileScanRDD])

    }

    withTempPath { path =>
      val tempDir = path.getCanonicalPath

      val df = createSimpleFileDeltaDF(spark, tempDir)
      assert(getFileSourceScanExec(df).isInstanceOf[KylinStorageScanExec])
      assert(getFileSourceScanExec(df).asInstanceOf[KylinStorageScanExec].inputRDD.isInstanceOf[CacheFileScanRDD])
    }

    spark.sparkContext.stop()
  }

  test("Create sharding read RDD without Soft affinity - FileScanRDD") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "false")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleFilePrunnerDF(spark, tempDir)

      assert(getFileSourceScanExec(df).asInstanceOf[KylinFileSourceScanExec].inputRDD.isInstanceOf[FileScanRDD])
      spark.sparkContext.stop()
    }
  }

  test("Create sharding read RDD with Soft affinity and Local cache - legacy in stream") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
        .config("spark.hadoop.spark.kylin.soft-affinity.enabled", "true")
        .config("spark.hadoop.fs.file.impl", "io.kyligence.kap.cache.kylin.OnlyForTestCacheFileSystem")
        .config("fs.file.impl.disable.cache", "true")
        .config("spark.extraListeners", "io.kyligence.kap.softaffinity.scheduler.SoftAffinityListener")
        .config("spark.hadoop.spark.kylin.local-cache.enabled", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.legacy.file-input-stream", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.buffer.file-input-stream", "false")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleDF(spark, tempDir)
      checkAnswer(df, Seq(Row(0, 6), Row(1, 4), Row(2, 10)))
      spark.sparkContext.stop()
    }
  }

  test("Create sharding read RDD with Soft affinity and Local cache - buffer in stream") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[1]")
        .config(SoftAffinityConstants.PARAMS_KEY_SOFT_AFFINITY_ENABLED, "true")
        .config("spark.hadoop.spark.kylin.soft-affinity.enabled", "true")
        .config("spark.hadoop.fs.file.impl", "io.kyligence.kap.cache.kylin.OnlyForTestCacheFileSystem")
        .config("fs.file.impl.disable.cache", "true")
        .config("spark.extraListeners", "io.kyligence.kap.softaffinity.scheduler.SoftAffinityListener")
        .config("spark.hadoop.spark.kylin.local-cache.enabled", "true")
        .config("spark.hadoop.spark.kylin.local-cache.use.legacy.file-input-stream", "false")
        .config("spark.hadoop.spark.kylin.local-cache.use.buffer.file-input-stream", "true")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      val df = createSimpleDF(spark, tempDir)
      checkAnswer(df, Seq(Row(0, 6), Row(1, 4), Row(2, 10)))
      spark.sparkContext.stop()
    }
  }

  private def createSimpleFilePrunnerDF(spark: SparkSession, tempDir: String) = {
    val df = createSimpleDF(spark, tempDir)
    val plan = df.queryExecution.logical
    val fp = Mockito.mock(classOf[FilePruner])
    Mockito.when(fp.listFilesInternal(ArgumentMatchers.any(),
      ArgumentMatchers.any(), ArgumentMatchers.any())).thenReturn(Seq.empty[PartitionDirectory])
    Mockito.when(fp.metadataOpsTimeNs).thenReturn(Some(0L))
    Mockito.when(fp.rootPaths).thenReturn(Seq.empty[Path])
    Dataset.ofRows(spark, replaceFileIndex(plan, fp))
  }

  private def createSimpleFileDeltaDF(spark: SparkSession, tempDir: String) = {
    val df = createSimpleDF(spark, tempDir)
    val plan = df.queryExecution.logical
    val fp = Mockito.mock(classOf[KylinDeltaLogFileIndex])
    Mockito.when(fp.listFiles(ArgumentMatchers.any(),
      ArgumentMatchers.any())).thenReturn(Seq.empty[PartitionDirectory])
    Mockito.when(fp.metadataOpsTimeNs).thenReturn(Some(0L))
    Mockito.when(fp.rootPaths).thenReturn(Seq.empty[Path])
    Mockito.when(fp.DeltaExpressionCache).thenReturn(CacheBuilder.newBuilder()
      .expireAfterAccess(12, TimeUnit.HOURS).build[(Seq[Expression], Seq[Expression]), (Seq[AddFile], Long)]())
    Dataset.ofRows(spark, replaceFileIndex(plan, fp))
  }

  private def createSimpleDF(spark: SparkSession, tempDir: String) = {
    spark.range(10)
      .selectExpr("id % 2 as a", "id % 3 as b", "id as c")
      .write
      .parquet(tempDir)

    spark.read.parquet(tempDir)
      .where("a = 0")
      .groupBy("b")
      .agg("c" -> "sum")
  }

  def replaceFileIndex(
                        target: LogicalPlan,
                        fileIndex: FileIndex): LogicalPlan = {
    target transform {
      case l@LogicalRelation(hfsr: HadoopFsRelation, _, _, _) =>
        l.copy(relation = hfsr.copy(location = fileIndex)(hfsr.sparkSession))
    }
  }

  private def getFileSourceScanExec(df: DataFrame) = {
    collectFirst(df.queryExecution.executedPlan) {
      case p: KylinFileSourceScanExec => p
      case p: KylinStorageScanExec => p
      case p: LayoutFileSourceScanExec => p
    }.get
  }

  protected def clearSparkSession(): Unit = {
    SparkSession.setActiveSession(null)
    SparkSession.setDefaultSession(null)
    SparkSession.cleanupAnyExistingSession()
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    QueryTest.checkAnswer(analyzedDF, expectedAnswer, true)
  }

  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

}
