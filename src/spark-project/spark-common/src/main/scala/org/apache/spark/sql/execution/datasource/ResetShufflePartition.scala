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
package org.apache.spark.sql.execution.datasource

import org.apache.kylin.common.{KapConfig, KylinConfig, QueryContext}
import org.apache.kylin.engine.spark.utils.StorageUtils.MB
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf

trait ResetShufflePartition extends Logging {

  def setShufflePartitions(bytes: Long, sourceRows: Long, sparkSession: SparkSession): Unit = {
    QueryContext.current().getMetrics.setSourceScanBytes(bytes)
    QueryContext.current().getMetrics.setSourceScanRows(sourceRows)
    val defaultParallelism = sparkSession.sparkContext.defaultParallelism
    val kapConfig = KapConfig.getInstanceFromEnv
    val partitionsNum = if (kapConfig.getSparkSqlShufflePartitions != -1) {
      kapConfig.getSparkSqlShufflePartitions
    } else {
      Math.min(QueryContext.current().getMetrics.getSourceScanBytes / (
        KylinConfig.getInstanceFromEnv.getQueryPartitionSplitSizeMB * 1024 * 1024) + 1,
        defaultParallelism).toInt
    }
    val originPartitionsNum = QueryContext.current().getShufflePartitionsReset
    if (partitionsNum > originPartitionsNum) {
      sparkSession.sessionState.conf.setLocalProperty(SQLConf.SHUFFLE_PARTITIONS.key, partitionsNum.toString)
      QueryContext.current().setShufflePartitionsReset(partitionsNum)
      logInfo(s"Set partition from $originPartitionsNum to $partitionsNum, " +
        s"total bytes ${QueryContext.current().getMetrics.getSourceScanBytes}")
    } else {
      logInfo(s"Origin partition is $originPartitionsNum, new partition is $partitionsNum, total bytes " +
        s"${QueryContext.current().getMetrics.getSourceScanBytes}, will not reset the ${SQLConf.SHUFFLE_PARTITIONS.key}")
    }
  }

  def needSetFilesMaxPartitionBytes(totalRowCount: Long, sourceBytes: Long, kapConfig: KapConfig): Boolean = {
    if (kapConfig.isResetMaxPartitionBytes) {
      val parquetRowCountThresholdSize = kapConfig.getQueryParquetRowCountThresholdSize
      val filesThresholdBytes = kapConfig.getQueryParquetFilesThresholdBytes
      val parquetRowCountPerMb = kapConfig.getQueryParquetRowCountPerMb
      // setFilesMaxPartitionBytes:
      // 1. sourceBytes < files threshold bytes
      // 2. totalRowCount < threshold size
      // 3. per file size < threshold per file size
      return (sourceBytes < filesThresholdBytes) &&
        (totalRowCount < parquetRowCountThresholdSize) &&
        ((totalRowCount / (sourceBytes * 1.0 / MB)) > parquetRowCountPerMb)
    }
    false
  }

  def setFilesMaxPartitionBytes(sourceBytes: Long, sourceRows: Long, sparkSession: SparkSession): Unit = {
    val kapConfig = KapConfig.getInstanceFromEnv
    val needSet = needSetFilesMaxPartitionBytes(sourceRows, sourceBytes, kapConfig)

    if (needSet) {
      val filesMaxPartitionBytes = kapConfig.getQueryFilesMaxPartitionBytes
      sparkSession.sessionState.conf.setLocalProperty(SQLConf.FILES_MAX_PARTITION_BYTES.key, filesMaxPartitionBytes.toString)
      logInfo(s"Set the ${SQLConf.FILES_MAX_PARTITION_BYTES.key} to $filesMaxPartitionBytes, " +
        s"total bytes $sourceBytes , total row count $sourceRows")
    } else {
      logInfo(s"Not need reset the ${SQLConf.FILES_MAX_PARTITION_BYTES.key}, " +
        s"total bytes $sourceBytes , total row count $sourceRows")
    }
  }
}
