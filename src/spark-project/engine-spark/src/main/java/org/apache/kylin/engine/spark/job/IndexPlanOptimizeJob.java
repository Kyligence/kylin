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

import static org.apache.kylin.engine.spark.job.StageEnum.OPTIMIZE_INDEX_PLAN;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.application.SparkApplication;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;

import lombok.val;

public class IndexPlanOptimizeJob extends SparkApplication {

    private String dataflowId;

    private NDataModel model;

    private IndexPlan indexPlan;

    private NDataflow dataflow;

    @Override
    protected void extraInit() {
        super.extraInit();
        // model id, index plan id and dataflow id are the same.
        dataflowId = getParam(NBatchConstants.P_DATAFLOW_ID);

        // data model
        NDataModelManager modelMgr = NDataModelManager.getInstance(config, project);
        model = modelMgr.getDataModelDesc(dataflowId);

        // index plan
        NIndexPlanManager indexPlanMgr = NIndexPlanManager.getInstance(config, project);
        indexPlan = indexPlanMgr.getIndexPlan(dataflowId);

        // dataflow
        NDataflowManager dataflowMgr = NDataflowManager.getInstance(config, project);
        dataflow = dataflowMgr.getDataflow(dataflowId);

        // disable fact view materialization
        disableConfig("kylin.engine.persist-flatview");
        // disable flat table materialization
        disableConfig("kylin.engine.persist-flattable-enabled");
        // disable source usage collection
        disableConfig("kylin.source.record-source-usage-enabled");
    }

    @Override
    protected void doExecute() throws Exception {
        val stepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val step = new StepExec(stepId);
        step.addStage(OPTIMIZE_INDEX_PLAN.createExec(this));
        step.doExecute();
    }

    public String getDataflowId() {
        return dataflowId;
    }

    public NDataModel getModel() {
        return model;
    }

    public IndexPlan getIndexPlan() {
        return indexPlan;
    }

    public NDataflow getDataflow() {
        return dataflow;
    }

    private void disableConfig(String key) {
        config.setProperty(key, "false");
    }

    public static void main(String[] args) {
        IndexPlanOptimizeJob indexPlanOptimizeJob = new IndexPlanOptimizeJob();
        indexPlanOptimizeJob.execute(args);
    }
}
