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
package org.apache.kylin.cluster

import io.fabric8.kubernetes.api.model.{Quantity, ResourceQuota}
import io.fabric8.kubernetes.client.Config.autoConfigure
import io.fabric8.kubernetes.client.{ConfigBuilder, DefaultKubernetesClient, KubernetesClient}
import io.fabric8.volcano.client.DefaultVolcanoClient
import io.fabric8.volcano.scheduling.v1beta1.Queue
import okhttp3.Dispatcher
import org.apache.commons.collections.CollectionUtils
import org.apache.kylin.common.KylinConfig
import org.apache.kylin.common.util.{ByteUnit, SizeConvertUtil}
import org.apache.kylin.engine.spark.utils.ThreadUtils
import org.apache.kylin.guava30.shaded.common.collect.Lists
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._

class K8sClusterManager extends IClusterManager with Logging {

  import org.apache.kylin.cluster.K8sClusterManager._

  private val JOB_STEP_PREFIX = "job-step-"
  private val SPARK_ROLE = "spark-role"
  private val DRIVER = "driver"
  private val DEFAULT_NAMESPACE = "default"
  private var config: KylinConfig = null

  override def withConfig(config: KylinConfig): Unit = {
    this.config = config
  }

  override def fetchMaximumResourceAllocation: ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def fetchQueueAvailableResource(queueName: String): AvailableResource = {
    withKubernetesClient(config, kubernetesClient => {
      val volcanoClient = new DefaultVolcanoClient(kubernetesClient)
      val queue = try {
        volcanoClient.queues().list().getItems.stream().filter(_.getMetadata.getName.equals(queueName)).findFirst()
      } catch {
        case e: Throwable =>
          logWarning(s"Failed to get volcano queue with error '${e.getMessage}'.", e)
          null
      } finally {
        volcanoClient.close()
      }
      if (queue != null && queue.isPresent && queue.get().getSpec.getCapability != null) {
        return getAvailableResourceByVolcano(queue.get())
      } else {
        val quotas = kubernetesClient.resourceQuotas().list().getItems
        if (CollectionUtils.isNotEmpty(quotas)) {
          return getAvailableResourceByQuota(quotas)
        }
      }
    })
    AvailableResource(ResourceInfo(Int.MaxValue, 1000), ResourceInfo(Int.MaxValue, 1000))
  }

  def getAvailableResourceByVolcano(queue: Queue): AvailableResource = {
    val capability = queue.getSpec.getCapability
    val memoryBytes = Quantity.getAmountInBytes(capability.getOrDefault("memory", Quantity.parse(Int.MaxValue + "Gi")))
    val memory = SizeConvertUtil.byteStringAs(memoryBytes + "b", ByteUnit.MiB).toInt
    val cpu = getCpuByQuantity(capability.getOrDefault("cpu", Quantity.parse("1000")))
    val resource = AvailableResource(ResourceInfo(memory, cpu), ResourceInfo(memory, cpu))
    log.info("getAvailableResourceByVolcano:{}", resource)
    resource
  }

  def getAvailableResourceByQuota(quotas: util.List[ResourceQuota]): AvailableResource = {
    val hardMemoryBytes = quotas.stream().mapToLong(quota => Quantity.getAmountInBytes(
      quota.getStatus.getHard.getOrDefault("limits.memory", Quantity.parse(Int.MaxValue + "Gi"))).longValue()) //
      .min().getAsLong

    val hardCpu = quotas.stream().mapToInt(quota => getCpuByQuantity(
      quota.getStatus.getHard.getOrDefault("limits.cpu", Quantity.parse("1000"))))
      .min().getAsInt

    val used = quotas.get(0).getStatus.getUsed
    val hardMemory = SizeConvertUtil.byteStringAs(hardMemoryBytes + "b", ByteUnit.MiB).toInt
    val usedMemoryBytes = Quantity.getAmountInBytes(used.getOrDefault("limits.memory", Quantity.parse("0Gi")))
    val usedMemory = SizeConvertUtil.byteStringAs(usedMemoryBytes + "b", ByteUnit.MiB).toInt

    val usedCpu = used.getOrDefault("limits.cpu", Quantity.parse("0")).getAmount.toInt
    val resource = AvailableResource(ResourceInfo(hardMemory - usedMemory, hardCpu - usedCpu), ResourceInfo(hardMemory, hardCpu))
    log.info("getAvailableResourceByQuota:{}", resource)
    resource
  }

  def getCpuByQuantity(quantity: Quantity): Int = {
    val format = quantity.getFormat
    if (format != null && format.equalsIgnoreCase("m")) {
      return quantity.getAmount.toInt / 1000
    }
    quantity.getAmount.toInt
  }

  override def getBuildTrackingUrl(sparkSession: SparkSession): String = {
    val applicationId = sparkSession.sparkContext.applicationId
    val trackUrl = sparkSession.sparkContext.uiWebUrl.getOrElse("")
    logInfo(s"Tracking Ur $applicationId from spark context $trackUrl ")
    return trackUrl
  }

  override def killApplication(jobStepId: String): Unit = {
    logInfo(s"Kill Application $jobStepId !")
    killApplication(s"$JOB_STEP_PREFIX", jobStepId)
  }

  override def killApplication(jobStepPrefix: String, jobStepId: String): Unit = {
    logInfo(s"Kill Application $jobStepPrefix $jobStepId !")
    withKubernetesClient(config, kubernetesClient => {
      val pName = jobStepPrefix + jobStepId
      val pods = getPods(pName, kubernetesClient)
      if (!pods.isEmpty) {
        val ops = kubernetesClient
          .pods
          .inNamespace(kubernetesClient.getNamespace)
        logInfo(s"delete pod $pods")
        ops.delete(pods.asJava)
      }
    })
  }

  override def isApplicationBeenKilled(jobStepId: String): Boolean = {
    withKubernetesClient(config, kubernetesClient => {
      val pods = getPods(jobStepId, kubernetesClient)
      pods.isEmpty
    })
  }

  override def getRunningJobs(queues: util.Set[String]): util.List[String] = {
    Lists.newArrayList()
  }

  override def fetchQueueStatistics(queueName: String): ResourceInfo = {
    ResourceInfo(Int.MaxValue, 1000)
  }

  override def applicationExisted(jobStepId: String): Boolean = {
    withKubernetesClient(config, kubernetesClient => {
      val pName = s"$JOB_STEP_PREFIX" + jobStepId
      val pods = getPods(pName, kubernetesClient)
      !pods.isEmpty
    })
  }

  private def getPods(pName: String, kubernetesClient: KubernetesClient) = {
    val ops = kubernetesClient
      .pods
      .inNamespace(kubernetesClient.getNamespace)
    val pods = ops
      .list()
      .getItems
      .asScala
      .filter { pod =>
        val meta = pod.getMetadata
        meta.getName.startsWith(pName) &&
          meta.getLabels.get(SPARK_ROLE) == DRIVER
      }.toList
    pods
  }

  override def getApplicationNameById(yarnAppId: Int): String = ""
}

object K8sClusterManager extends Logging {

  def withKubernetesClient[T](master: String, namespace: String, body: KubernetesClient => T): T = {
    val kubernetesClient = createKubernetesClient(master, namespace)
    try {
      body(kubernetesClient)
    } finally {
      kubernetesClient.close()
    }
  }

  def withKubernetesClient[T](config: KylinConfig, body: KubernetesClient => T): T = {
    val master = config.getSparkMaster.substring("k8s://".length)
    val namespace = config.getKubernetesNameSpace
    withKubernetesClient(master, namespace, body)
  }

  def createKubernetesClient(master: String,
                             namespace: String): KubernetesClient = {

    val dispatcher = new Dispatcher(
      ThreadUtils.newDaemonScalableThreadPool("kubernetes-dispatcher", 0, 100, 60L, TimeUnit.SECONDS))

    // Allow for specifying a context used to auto-configure from the users K8S config file
    val kubeContext = null

    // Now it is a simple config,
    // TODO enrich it if more config is needed
    val config = new ConfigBuilder(autoConfigure(kubeContext))
      .withApiVersion("v1")
      .withMasterUrl(master)
      .withWebsocketPingInterval(0)
      .withNamespace(namespace)
      .withRequestTimeout(10000)
      .withConnectionTimeout(10000)
      .build()
    new DefaultKubernetesClient(config)
  }
}