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

import org.apache.spark.internal.Logging

import scala.collection.mutable

class ContainerInitializeListener extends SparkListener with Logging {

  override def onExecutorAdded(executorAdded: SparkListenerExecutorAdded): Unit = {
    if (ContainerInitializeListener.needInitialize) {
      ContainerInitializeListener.executorIds += executorAdded.executorId
      if (ContainerInitializeListener.executorIds.size > 10000) {
        log.warn("Executor has exceeded 1000")
        ContainerInitializeListener.needInitialize = false
      }
    }
  }
}

object ContainerInitializeListener {
  @volatile
  private var needInitialize: Boolean = true
  val executorIds = new mutable.HashSet[String]()

  def start(): Unit = {
    needInitialize = true;
    executorIds.clear()
  }

  def stop(): Unit = {
    needInitialize = false;
    executorIds.clear()
  }
}
