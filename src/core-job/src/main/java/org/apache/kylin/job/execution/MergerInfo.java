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

package org.apache.kylin.job.execution;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.kylin.job.util.ExecutableParaUtil;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MergerInfo {

    private String project;
    private String toBeDeleteLayoutIdsStr;
    private String modelId;
    private String jobId;
    private int errorOrPausedJobCount;
    private ExecutableHandler.HandlerType handlerType;
    private List<TaskMergeInfo> taskMergeInfoList = new ArrayList<>();

    public MergerInfo(String project, String toBeDeleteLayoutIdsStr, String modelId, String jobId,
            int errorOrPausedJobCount, ExecutableHandler.HandlerType handlerType) {
        this(project, modelId, jobId, errorOrPausedJobCount, handlerType);
        this.toBeDeleteLayoutIdsStr = toBeDeleteLayoutIdsStr;
    }

    public MergerInfo(String project, String modelId, String jobId, int errorOrPausedJobCount,
            ExecutableHandler.HandlerType handlerType) {
        this(project, handlerType);
        this.modelId = modelId;
        this.jobId = jobId;
        this.errorOrPausedJobCount = errorOrPausedJobCount;
    }

    public MergerInfo(String project, ExecutableHandler.HandlerType handlerType) {
        this.project = project;
        this.handlerType = handlerType;
    }

    public void addTaskMergeInfo(AbstractExecutable job, boolean needBuildSnapshots) {
        this.taskMergeInfoList.add(new TaskMergeInfo(job, needBuildSnapshots));
    }

    public void addTaskMergeInfo(AbstractExecutable job) {
        this.taskMergeInfoList.add(new TaskMergeInfo(job));
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class TaskMergeInfo {
        private String outputMetaUrl;
        // Used for AfterBuildResourceMerger & AfterMergeOrRefreshResourceMerger
        private String dataFlowId;
        private Set<String> segmentIds;
        private Set<Long> layoutIds;
        private Set<Long> partitionIds;
        private JobTypeEnum jobType;
        private boolean needBuildSnapshots;
        // Used for AfterSamplingMerger & AfterSnapshotMerger
        private String tableIdentity;
        private long createTime;
        private String selectPartCol;
        private boolean incrementBuild;

        public TaskMergeInfo(AbstractExecutable job, boolean needBuildSnapshots) {
            this.outputMetaUrl = ExecutableParaUtil.getOutputMetaUrl(job);
            this.dataFlowId = ExecutableParaUtil.getDataflowId(job);
            this.segmentIds = ExecutableParaUtil.getSegmentIds(job);
            this.layoutIds = ExecutableParaUtil.getLayoutIds(job);
            this.partitionIds = ExecutableParaUtil.getPartitionIds(job);
            this.jobType = job.getJobType();
            this.needBuildSnapshots = needBuildSnapshots;
        }

        public TaskMergeInfo(AbstractExecutable job) {
            this.outputMetaUrl = ExecutableParaUtil.getOutputMetaUrl(job);
            this.tableIdentity = ExecutableParaUtil.getTableIdentity(job);
            this.createTime = job.getCreateTime();
            this.selectPartCol = ExecutableParaUtil.getSelectPartCol(job);
            this.incrementBuild = ExecutableParaUtil.isIncrementBuild(job);
        }
    }
}
