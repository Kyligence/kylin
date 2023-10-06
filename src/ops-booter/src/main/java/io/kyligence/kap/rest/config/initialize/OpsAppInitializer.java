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

package io.kyligence.kap.rest.config.initialize;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.service.OpsService;
import org.apache.kylin.tool.MaintainModeTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;

@Configuration
public class OpsAppInitializer {
    public final Logger log = LoggerFactory.getLogger(OpsAppInitializer.class);

    @EventListener(ApplicationReadyEvent.class)
    public void beforeStarted() throws IOException {
        checkMetadataRestoreTaskStatus();
        checkMaintainMode();
        checkMetadataBackupTaskStatus();
    }

    public void checkMetadataRestoreTaskStatus() {
        log.info("start to check metadata restore task status.");
        long startTime = System.currentTimeMillis();
        AsyncTaskManager manger = AsyncTaskManager.getInstance("");
        List<AbstractAsyncTask> asyncTask = manger.getAllAsyncTaskByType(AsyncTaskManager.METADATA_RECOVER_TASK);
        for (AbstractAsyncTask abstractAsyncTask : asyncTask) {
            MetadataRestoreTask task = MetadataRestoreTask.copyFromAbstractTask(abstractAsyncTask);
            if (MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS.equals(task.getStatus())) {
                log.info("mark in progress metadata restore task {} as failed.", task.getTaskKey());
                task.setStatus(MetadataRestoreTask.MetadataRestoreStatus.FAILED);
                manger.save(task);
            }
        }
        log.info("finished check metadata restore task status in {} ms", System.currentTimeMillis() - startTime);
    }

    public void checkMaintainMode() {
        if (EpochManager.getInstance().isMaintenanceMode()) {
            log.info("start to exit maintain mode.");
            long startTime = System.currentTimeMillis();
            MaintainModeTool maintainModeTool = new MaintainModeTool();
            maintainModeTool.init();
            maintainModeTool.releaseEpochs();
            log.info("finished exit maintain mode in {} ms.", System.currentTimeMillis() - startTime);
        }
    }

    public void checkMetadataBackupTaskStatus() throws IOException {
        log.info("start to check metadata backup status");
        long startTime = System.currentTimeMillis();
        List<String> projectList = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                .stream().map(ProjectInstance::toString).collect(Collectors.toList());
        projectList.add(OpsService._GLOBAL);
        for (String project : projectList) {
            for (MetadataBackupResponse metadataBackup : OpsService.getMetadataBackupList(project)) {
                if (OpsService.MetadataBackupStatu.IN_PROGRESS.equals(metadataBackup.getStatus())) {
                    OpsService.MetadataBackupOperator operator = new OpsService.MetadataBackupOperator(metadataBackup,
                            project);
                    operator.markFail();
                }
            }
        }
        log.info("finished check metadata backup status in {} ms", System.currentTimeMillis() - startTime);
    }
}
