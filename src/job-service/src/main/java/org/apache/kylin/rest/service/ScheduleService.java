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
package org.apache.kylin.rest.service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.metadata.epoch.EpochManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.tool.garbage.SourceUsageCleaner;
import org.apache.kylin.tool.routine.FastRoutineTool;
import org.apache.kylin.tool.routine.RoutineTool;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ScheduleService {

    private static final String GLOBAL = "global";

    @Autowired
    MetadataBackupService backupService;

    @Autowired
    ProjectService projectService;

    @Autowired(required = false)
    ProjectSmartSupporter projectSmartSupporter;

    private final ExecutorService executors = Executors
            .newSingleThreadExecutor(new NamedThreadFactory("RoutineTaskScheduler"));

    private long opsCronTimeout;

    private static final ThreadLocal<Future<?>> CURRENT_FUTURE = new ThreadLocal<>();

    @Scheduled(cron = "${kylin.metadata.ops-cron:0 0 0 * * *}")
    public void routineTask() throws Exception {
        opsCronTimeout = KylinConfig.getInstanceFromEnv().getRoutineOpsTaskTimeOut();
        CURRENT_FUTURE.remove();
        long startTime = System.currentTimeMillis();
        EpochManager epochManager = EpochManager.getInstance();
        try {
            MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);
            try (SetThreadName ignored = new SetThreadName("RoutineOpsWorker")) {
                log.info("Start to work");
                if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                    executeTask(() -> backupService.backupAll(), "MetadataBackup", startTime);
                    executeTask(RoutineTool::cleanQueryHistories, "QueryHistoriesCleanup", startTime);
                    executeTask(RoutineTool::cleanStreamingStats, "StreamingStatsCleanup", startTime);
                    executeTask(RoutineTool::deleteRawRecItems, "RawRecItemsDeletion", startTime);
                    executeTask(() -> EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                        new SourceUsageCleaner().cleanup();
                        return null;
                    }, UnitOfWork.GLOBAL_UNIT), "SourceUsageCleanup", startTime);
                }
                executeTask(() -> projectService.garbageCleanup(getRemainingTime(startTime)), "GarbageCleanup",
                        startTime);
                executeTask(() -> newFastRoutineTool().execute(new String[] { "-c" }), "HdfsCleanup", startTime);
                log.info("Finish to work");
            }
        } catch (InterruptedException e) {
            log.error("Routine task execution interrupted", e);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            log.error("Routine task execution timeout", e);
            if (CURRENT_FUTURE.get() != null) {
                CURRENT_FUTURE.get().cancel(true);
            }
        }
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
    }

    public void executeTask(Runnable task, String taskName, long startTime)
            throws InterruptedException, ExecutionException, TimeoutException {
        val future = executors.submit(task);
        val remainingTime = getRemainingTime(startTime);
        log.info("execute task {} with remaining time: {} ms", taskName, remainingTime);
        CURRENT_FUTURE.set(future);
        future.get(remainingTime, TimeUnit.MILLISECONDS);
    }

    private long getRemainingTime(long startTime) {
        return opsCronTimeout - (System.currentTimeMillis() - startTime);
    }

    public FastRoutineTool newFastRoutineTool() {
        return new FastRoutineTool();
    }
}
