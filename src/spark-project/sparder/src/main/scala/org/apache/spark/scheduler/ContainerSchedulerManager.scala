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

package org.apache.spark.scheduler

import org.apache.kylin.common.KylinConfig
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.resource.{ResourceProfile, ResourceProfileManager}
import org.apache.spark.util.{SystemClock, Utils}
import org.apache.spark.{ContextCleaner, ExecutorAllocationClient, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class ContainerSchedulerManager(client: ExecutorAllocationClient,
                                listenerBus: LiveListenerBus,
                                conf: SparkConf,
                                cleaner: Option[ContextCleaner] = None,
                                resourceProfileManager: ResourceProfileManager) extends Logging {

  private val kylinConfig = KylinConfig.getInstanceFromEnv

  private val executorListener = new ExecutorListener(kylinConfig, client, listenerBus, new SystemClock())


  private val initialExecutors = conf.get(EXECUTOR_INSTANCES).getOrElse(0)
  private val defaultProfileId = resourceProfileManager.defaultResourceProfile.id
  private val numExecutorsTargetPerResourceProfileId = new mutable.HashMap[Int, Int]
  numExecutorsTargetPerResourceProfileId(defaultProfileId) = initialExecutors
  // Number of locality aware tasks for each ResourceProfile, used for executor placement.
  private val numLocalityAwareTasksPerResourceProfileId = new mutable.HashMap[Int, Int]
  numLocalityAwareTasksPerResourceProfileId(defaultProfileId) = 0
  private val rpIdToHostToLocalTaskCount: Map[Int, Map[String, Int]] = Map.empty


  private val executorMemory = conf.get(EXECUTOR_MEMORY)
  private val executorOffHeapMemory = Utils.executorOffHeapMemorySizeAsMb(conf)
  private val executorMemoryOverhead = conf.get(EXECUTOR_MEMORY_OVERHEAD).getOrElse(
    math.max((conf.get(EXECUTOR_MEMORY_OVERHEAD_FACTOR) * executorMemory).toLong,
      ResourceProfile.MEMORY_OVERHEAD_MIN_MIB)).toInt


  def getExecutorMemory: Long = {
    math.max(executorMemory + executorMemoryOverhead + executorOffHeapMemory, kylinConfig.getContainerMinMB)
  }

  private val executorCores = conf.get(EXECUTOR_CORES)


  def getExecutorCores: Int = {
    math.max(executorCores, kylinConfig.getContainerMinCore)
  }

  def start(): Unit = {
    listenerBus.addToManagementQueue(executorListener)
    cleaner.foreach(_.attachListener(executorListener))
    ContainerInitializeListener.executorIds.foreach(executorListener.ensureExecutorIsTracked)
    ContainerInitializeListener.stop()
  }

  def getAllExecutorCores: Int = {
    executorCores * executorListener.executorCount
  }

  def getAllExecutorMemory: Long = {
    getExecutorMemory * executorListener.executorCount
  }

  def getExecutorCount: Int = {
    executorListener.executorCount
  }

  def requestExecutor(num: Int): Boolean = {
    this.synchronized {
      val numExisting = numExecutorsTargetPerResourceProfileId.getOrElse(defaultProfileId, 0)
      numExecutorsTargetPerResourceProfileId(defaultProfileId) = numExisting + num
    }
    client.requestTotalExecutors(
      numExecutorsTargetPerResourceProfileId.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap,
      rpIdToHostToLocalTaskCount)
  }

  def releaseExecutor(num: Int, force: Boolean): Seq[String] = {
    val executors = if (force) executorListener.noPendingRemovalExecutors() else executorListener.timedOutExecutors()

    val executorIdsToBeRemoved = new ArrayBuffer[String]
    var flag = true;
    for (executor <- executors if flag) {
      executorIdsToBeRemoved += executor
      if (executorIdsToBeRemoved.size == num) {
        flag = false
      }
    }

    if (executorIdsToBeRemoved.isEmpty) {
      return Seq.empty[String]
    }
    val rs = client.killExecutors(executorIdsToBeRemoved, false, false)
    this.synchronized {
      val numExisting = numExecutorsTargetPerResourceProfileId.getOrElse(defaultProfileId, 0)
      numExecutorsTargetPerResourceProfileId(defaultProfileId) = numExisting - rs.size
    }
    client.requestTotalExecutors(
      numExecutorsTargetPerResourceProfileId.toMap,
      numLocalityAwareTasksPerResourceProfileId.toMap,
      rpIdToHostToLocalTaskCount)
    executorListener.executorsKilled(rs)
    rs
  }

  def getQueueName: String = {
    var queue = conf.get("spark.kubernetes.executor.annotation.scheduling.kyligence.io.default-queue", null)
    if (queue == null) {
      queue = conf.get("spark.kubernetes.executor.annotation.scheduling.volcano.sh/queue-name", null)
    }
    if (queue == null) {
      queue = conf.get("spark.yarn.queue", "default")
    }
    queue
  }

}