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

package org.apache.spark.sql.execution.gluten

import io.glutenproject.execution.FileSourceScanExecTransformer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{KylinFileSourceScanExec, LayoutFileSourceScanExec, SparkPlan}

case class ConvertKylinFileSourceToGlutenRule(session: SparkSession) extends Rule[SparkPlan] {

  override def apply(plan: SparkPlan): SparkPlan = plan.transformDown {
    case f: KylinFileSourceScanExec =>
      // convert to Gluten transformer
      val transformer = new KylinFileSourceScanExecTransformer(
        f.relation,
        f.output,
        f.requiredSchema,
        f.partitionFilters,
        None,
        f.optionalShardSpec,
        f.optionalNumCoalescedBuckets,
        f.dataFilters,
        f.tableIdentifier,
        f.disableBucketedScan,
        f.sourceScanRows
      )
      // Transformer validate
      if (transformer.doValidate().validated) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        f
      }

    case l: LayoutFileSourceScanExec =>
      // convert to Gluten transformer
      val transformer = new FileSourceScanExecTransformer(
        l.relation,
        l.output,
        l.requiredSchema,
        l.partitionFilters,
        l.optionalBucketSet,
        l.optionalNumCoalescedBuckets,
        l.dataFilters,
        l.tableIdentifier,
        l.disableBucketedScan
      )
      // Transformer validate
      if (transformer.doValidate().validated) {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently supported.")
        transformer
      } else {
        logDebug(s"Columnar Processing for ${plan.getClass} is currently unsupported.")
        l
      }
  }
}
