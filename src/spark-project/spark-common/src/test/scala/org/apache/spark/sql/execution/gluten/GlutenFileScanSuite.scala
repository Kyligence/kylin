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
package org.apache.spark.sql.execution.gluten

import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util.stackTraceToString
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.datasource.{KylinSourceStrategy, LayoutFileSourceStrategy}

class GlutenFileScanSuite extends SparkFunSuite
  with SQLHelper with AdaptiveSparkPlanHelper {

  override def beforeEach(): Unit = {
    clearSparkSession()
  }

  override def afterEach(): Unit = {
    clearSparkSession()
  }

  test("Test with Gluten") {
    withTempPath { path =>
      SparkSession.cleanupAnyExistingSession()
      val tempDir = path.getCanonicalPath
      val spark = SparkSession.builder()
        .master("local[3]")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.plugins", "org.apache.gluten.GlutenPlugin")
        .config("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
        .config("spark.io.compression.codec", "LZ4")
        .config("spark.gluten.sql.columnar.libpath", "/usr/local/clickhouse/lib/libch.so")
        .config("spark.gluten.sql.enable.native.validation", "false")
        .config("spark.memory.offHeap.enabled", "true")
        .config("spark.memory.offHeap.size", "2G")
        .config("spark.gluten.sql.columnar.extended.columnar.pre.rules",
          "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule")
        .config("spark.gluten.sql.columnar.extended.columnar.post.rules", "")
        .withExtensions { ext =>
          ext.injectPlannerStrategy(_ => KylinSourceStrategy)
          ext.injectPlannerStrategy(_ => LayoutFileSourceStrategy)
        }
        .getOrCreate()

      withSQLConf(("spark.gluten.enabled", "false")) {
        createSimpleDF(spark, tempDir)
      }
      val df = spark.read.parquet(tempDir)
        .where("a = 0")
        .groupBy("b")
        .agg("c" -> "sum")

      val res = df.collect()
      assert(res.size == 3)
      val f = getNativeFileSourceScanExecTransformer(df)
      assert(!f.isEmpty)
      assert(f.get.isInstanceOf[FileSourceScanExecTransformer])
      spark.sparkContext.stop()
    }
  }

  private def createSimpleDF(spark: SparkSession, tempDir: String) = {
    spark.range(100)
      .selectExpr("id % 2 as a", "id % 3 as b", "id as c")
      .write
      .parquet(tempDir)
  }

  private def getNativeFileSourceScanExecTransformer(df: DataFrame) = {
    collectFirst(df.queryExecution.executedPlan) {
      case p: KylinFileSourceScanExecTransformer => p
      case p: FileSourceScanExecTransformer => p
    }
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
