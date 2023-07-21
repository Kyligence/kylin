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
package org.apache.spark.utils

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.deploy.SparkHadoopUtil

/**
 * Convenience utility object for invoking [[SparkHadoopUtil]].
 */
object SparkHadoopUtils {

  /**
   * Simply return a new default hadoop [[Configuration]].
   * @return Newly created default hadoop configuration
   */
  def newConfiguration(): Configuration = {
    new Configuration()
  }

  /**
   * Returns a new hadoop [[Configuration]] with current [[SparkConf]] from [[SparkEnv]].
   * @return Newly created hadoop configuration with extra spark properties
   */
  def newConfigurationWithSparkConf(): Configuration = {
    val sparkEnv = SparkEnv.get
    if (sparkEnv == null) {
      throw new IllegalStateException("sparkEnv should not be null")
    }
    SparkHadoopUtil.newConfiguration(sparkEnv.conf)
  }

  /**
   * Returns a new hadoop [[Configuration]] with [[SparkConf]] given.
   * @param sparkConf A [[SparkConf]]
   * @return Newly created hadoop configuration with extra spark properties given
   */
  def newConfigurationWithSparkConf(sparkConf: SparkConf): Configuration = {
    SparkHadoopUtil.newConfiguration(sparkConf)
  }
}
