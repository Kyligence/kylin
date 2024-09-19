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

package org.apache.kylin.engine.spark.job;

import static org.apache.kylin.engine.spark.job.StageEnum.DELETE_USELESS_LAYOUT_DATA;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_COMPACTION;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_REPARTITION;
import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_LAYOUT_DATA_BY_ZORDER;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.job.exec.OptimizeExec;

import lombok.val;

public class LayoutDataOptimizeJob extends AbstractLayoutDataOptimizeJob {

    public static void main(String[] args) {
        LayoutDataOptimizeJob layoutDataOptimizeJob = new LayoutDataOptimizeJob();
        layoutDataOptimizeJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val step = new OptimizeExec(jobStepId);

        step.addStage(DELETE_USELESS_LAYOUT_DATA.createExec(this));
        step.addStage(OPTIMIZE_LAYOUT_DATA_BY_REPARTITION.createExec(this));
        step.addStage(OPTIMIZE_LAYOUT_DATA_BY_ZORDER.createExec(this));
        step.addStage(OPTIMIZE_LAYOUT_DATA_BY_COMPACTION.createExec(this));

        step.optimize();
    }
}