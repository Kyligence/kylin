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

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.http.client.utils.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.IndexDataConstructor;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTest;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.handler.IndexPlanOptimizeJobHandler;
import org.apache.kylin.job.model.JobParam;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NBatchConstants;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.cube.model.RuleBasedIndex;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class NSparkIndexPlanOptimizeJobTest extends NLocalWithSparkSessionTest {
    private KylinConfig config;
    private String modelUuid = "b780e4e4-69af-449e-b09f-05c90dfa04b6";

    @Before
    @Override
    public void setUp() throws Exception {
        JobContextUtil.cleanUp();
        super.setUp();
        ss.sparkContext().setLogLevel("ERROR");
        config = getTestConfig();
        JobContextUtil.getJobContext(config);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        JobContextUtil.cleanUp();
        super.tearDown();
    }

    @Test
    public void testIndexPlanOptimize() throws InterruptedException {
        List<RawRecItem> recs = runIndexPlanOptimize(true);
        Assert.assertEquals(2, recs.size());
    }

    @Test
    public void testIndexPlanOptimizeWithQueryHistory() throws InterruptedException, IOException {
        prepareQueryHistory();
        List<RawRecItem> recs = runIndexPlanOptimize(true);
        Assert.assertEquals(4, recs.size());
        Assert.assertTrue(
                recs.stream().anyMatch(rec -> Arrays.toString(rec.getDependIDs()).equals("[0, 1, 2, -1, 100001, -2]")));
    }

    @Test
    public void testAutoRemoveExistingIndex() throws IOException, InterruptedException {
        prepareQueryHistory();
        List<RawRecItem> recs = runIndexPlanOptimize(false);
        Assert.assertTrue(recs.stream().anyMatch(rec -> rec.getType() == RawRecItem.RawRecType.REMOVAL_LAYOUT));
    }

    private List<RawRecItem> runIndexPlanOptimize(boolean dropExistingIndex) throws InterruptedException {
        String project = getProject();
        if (dropExistingIndex) {
            dropAllIndex(modelUuid, project);
        }

        NSparkIndexPlanOptimizeJob job = createJob(project, modelUuid, 2, 4);
        ExecutableManager execMgr = ExecutableManager.getInstance(config, getProject());
        execMgr.addJob(job);
        ExecutableState status = IndexDataConstructor.wait(job);
        Assert.assertEquals(ExecutableState.SUCCEED, status);
        RawRecManager recManager = RawRecManager.getInstance(project);
        return recManager.queryAll().stream().filter(rec -> rec.getModelID().equals(modelUuid))
                .collect(Collectors.toList());
    }

    private void prepareQueryHistory() throws IOException {
        String workingDir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(getProject());
        if (workingDir.startsWith("file:")) {
            workingDir = workingDir.substring(5);
        }
        String current_day = DateUtils.formatDate(new Date(), "yyyy-MM-dd");
        String moduleDir = new File("").getAbsolutePath();
        File sourceFile = new File(moduleDir + "/src/test/resources/query_rec");
        File targetFile = new File(workingDir, "query_rec/history/" + current_day);
        FileUtils.copyDirectory(sourceFile, targetFile);
    }

    private NSparkIndexPlanOptimizeJob createJob(String project, String modelId, int maxChange, int maxTotal) {
        JobParam jobParam = new JobParam(modelId, "admin").withProject(project)
                .withJobTypeEnum(JobTypeEnum.INDEX_PLAN_OPT)
                .addExtParams(NBatchConstants.P_DATAFLOW_ID, String.valueOf(modelId))
                .addExtParams(NBatchConstants.P_PLANNER_DATA_RANGE_START, String.valueOf(0L))
                .addExtParams(NBatchConstants.P_PLANNER_DATA_RANGE_END, String.valueOf(0L));
        jobParam.addExtParams(NBatchConstants.P_PLANNER_INITIALIZE_CUBOID_COUNT, String.valueOf(maxChange));
        jobParam.addExtParams(NBatchConstants.P_PLANNER_MAX_CUBOID_COUNT, String.valueOf(maxTotal));
        jobParam.addExtParams(NBatchConstants.P_PLANNER_MAX_CUBOID_CHANGE_COUNT, String.valueOf(maxChange));
        return NSparkIndexPlanOptimizeJob.create(new IndexPlanOptimizeJobHandler.IndexPlanOptimizeJobParam(jobParam));
    }

    private void dropAllIndex(String uuid, String project) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            KylinConfig conf = KylinConfig.getInstanceFromEnv();
            NIndexPlanManager manager = NIndexPlanManager.getInstance(conf, project);
            manager.updateIndexPlan(uuid, copyForWrite -> {
                copyForWrite.setIndexes(Collections.emptyList());
                copyForWrite.setRuleBasedIndex(new RuleBasedIndex());
            });
            return true;
        }, project);
    }
}
