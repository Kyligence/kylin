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
package org.apache.spark.sql.common

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext, SQLImplicits, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.util.Utils
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

trait SharedSparkSession
    extends BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {
  self: Suite =>
  @transient private var _sc: SparkContext = _
  @transient private var _spark: SparkSession = _
  @transient private var _jsc: JavaSparkContext = _
  var _conf: SparkConf = new SparkConf()
  val master: String = "local[4]"

  def sc: SparkContext = _sc

  protected implicit def spark: SparkSession = _spark

  var conf = new SparkConf(false)

  override def beforeAll() {
    super.beforeAll()
    val file = new File("./spark-warehouse")
    if (file.exists()) {
      FileUtils.deleteDirectory(file)
    }

    // Copied from TestHive
    // HDFS root scratch dir requires the write all (733) permission. For each connecting user,
    // an HDFS scratch dir: ${hive.exec.scratchdir}/<username> is created, with
    // ${hive.scratch.dir.permission}. To resolve the permission issue, the simplest way is to
    // delete it. Later, it will be re-created with the right permission.
    val scratchDir = Utils.createTempDir()
    if(scratchDir.exists()) {
      FileUtils.deleteDirectory(scratchDir)
    }
    conf.set(ConfVars.SCRATCHDIR.varname, scratchDir.toString)
    initSpark()
  }

  def initSpark(): Unit = {
    configGluten()
    _spark = SparkSession.builder
      .master(master)
      .appName(getClass.getSimpleName)
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.columnVector.offheap.enabled", "true")
      .config("spark.memory.fraction", "0.1")
      .config("fs.file.impl", classOf[DebugFilesystem].getCanonicalName)
      .config("spark.sql.adaptive.enabled", "true")
      .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
      .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
      .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
      .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
      .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
      .config("spark.sql.parquet.mergeSchema", "true")
      .config("spark.sql.legacy.allowNegativeScaleOfDecimal", "true")
      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
      .config("spark.databricks.delta.vacuum.parallelDelete.enabled", "true")
      .config(conf)
      .getOrCreate
    _jsc = new JavaSparkContext(_spark.sparkContext)
    _sc = _spark.sparkContext
  }

  protected def configGluten(): Unit = {
    val chLibPath = "/usr/local/clickhouse/lib/libch.so"
    if (StringUtils.isEmpty(chLibPath) || !new File(chLibPath).exists) {
      log.warn("-Dclickhouse.lib.path is not set or path not exists, skip gluten config")
    }
    conf.set("spark.gluten.enabled", "true")
    conf.set("spark.plugins", "org.apache.gluten.GlutenPlugin")
    conf.set("spark.gluten.sql.columnar.libpath", chLibPath)
    conf.set("spark.gluten.sql.columnar.extended.columnar.pre.rules",
      "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule")
    conf.set("spark.gluten.sql.columnar.extended.expressions.transformer",
      "org.apache.spark.sql.catalyst.expressions.gluten.CustomerExpressionTransformer")
    // FIXME, enable this after we fix the issue of AutoSinaiPocTest and AutoTpchTest
    conf.set("spark.sql.adaptive.enabled", "false")
    conf.set("spark.sql.parquet.enableVectorizedReader", "true")
    conf.set("spark.sql.columnVector.offheap.enabled", "true")
    conf.set("spark.memory.offHeap.enabled", "true")
    conf.set("spark.memory.offHeap.size", "1g")
    conf.set("spark.gluten.sql.enable.native.validation", "false")
    conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager")
    conf.set("spark.gluten.sql.columnar.iterator", "true")
    conf.set("spark.gluten.sql.columnar.sort", "true")
    conf.set("spark.sql.exchange.reuse", "true")
    conf.set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true")
    conf.set("spark.locality.wait", "0")
    conf.set("spark.locality.wait.node", "0")
    conf.set("spark.locality.wait.process", "0")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB")
    conf.set("spark.gluten.sql.columnar.columnartorow", "true")
    conf.set("spark.gluten.sql.columnar.loadnative", "true")
    conf.set("spark.gluten.sql.columnar.loadarrow", "false")
    conf.set("spark.gluten.sql.columnar.hashagg.enablefinal", "true")
    conf.set("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false")
    conf.set("spark.databricks.delta.maxSnapshotLineageLength", "20")
    conf.set("spark.databricks.delta.snapshotPartitions", "1")
    conf.set("spark.databricks.delta.properties.defaults.checkpointInterval", "5")
    conf.set("spark.databricks.delta.stalenessLimit", "3600000")
    conf.set("spark.gluten.sql.columnar.backend.ch.worker.id", "1")
    conf.set("spark.gluten.sql.columnar.coalesce.batches", "false")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.logger.level", "error")
    conf.set("spark.io.compression.codec", "LZ4")
    conf.set("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "LZ4")
    conf.set("spark.gluten.sql.columnar.backend.ch.customized.shuffle.codec.enable", "true")
    conf.set("spark.gluten.sql.columnar.backend.ch.customized.buffer.size", "4096")
    conf.set("spark.gluten.sql.columnar.backend.ch.files.per.partition.threshold", "5")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.enable_nullable", "true")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.local_engine.settings.metrics_perf_events_enabled", "false")
    conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.local_engine.settings.max_bytes_before_external_group_by", "5000000000")
    conf.set("spark.gluten.sql.columnar.maxBatchSize", "65409")
    conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false")
  }

  protected object testImplicits extends SQLImplicits {
    protected override def _sqlContext: SQLContext = spark.sqlContext
  }

  override def afterAll(): Unit = {
    try {
      _spark.stop()
      _sc = null
    } finally {
      super.afterAll()
    }
  }

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    DebugFilesystem.clearOpenStreams()
  }

  protected override def afterEach(): Unit = {
    super.afterEach()
    DebugFilesystem.assertNoOpenStreams()
  }

  def sql(sql: String): DataFrame = {
    spark.sql(sql)
  }
  /**
    * Drops global temporary view `viewNames` after calling `f`.
    */
  protected def withGlobalTempView(viewNames: String*)(f: => Unit): Unit = {
    try f finally {
      // If the test failed part way, we don't want to mask the failure by failing to remove
      // global temp views that never got created.
      try viewNames.foreach(spark.catalog.dropGlobalTempView) catch {
        case _: NoSuchTableException =>
      }
    }
  }

  /**
    * Drops table `tableName` after calling `f`.
    */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
    * Drops view `viewName` after calling `f`.
    */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    try f finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }
}
