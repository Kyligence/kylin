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
package org.apache.kylin.rest.config.initialize;

import java.io.IOException;
import java.util.function.Consumer;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.scheduler.ProjectControlledNotifier;
import org.apache.kylin.common.scheduler.ProjectEscapedNotifier;
import org.apache.kylin.common.scheduler.SchedulerEventNotifier;
import org.apache.kylin.guava30.shaded.common.eventbus.Subscribe;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.service.UserAclService;
import org.apache.kylin.rest.service.UserService;
import org.apache.kylin.rest.service.task.QueryHistoryMetaUpdateScheduler;
import org.apache.kylin.rest.util.CreateAdminUserUtils;
import org.apache.kylin.rest.util.InitResourceGroupUtils;
import org.apache.kylin.rest.util.InitUserGroupUtils;
import org.apache.kylin.streaming.jobs.scheduler.StreamingScheduler;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class EpochChangedListener {

    private static final String GLOBAL = "_global";

    @Autowired
    Environment env;

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @Autowired
    @Qualifier("userAclService")
    UserAclService userAclService;

    @Subscribe
    public void onProjectControlled(ProjectControlledNotifier notifier) throws IOException {
        wrapForCallbackInvocation(notifier, eventNotifier -> {
            String project = notifier.getProject();
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            val epochManager = EpochManager.getInstance();
            if (!GLOBAL.equals(project)) {
                doOnProjectControlled(project, kylinConfig);
            } else {
                doOnGlobalControlled();
            }
        });
    }

    private void initSchedule(KylinConfig kylinConfig, String project) {
        StreamingScheduler ss = StreamingScheduler.getInstance(project);
        ss.init();
        if (!ss.getHasStarted().get()) {
            throw new RuntimeException("Streaming Scheduler for " + project + " has not been started");
        }
    }

    @Subscribe
    public void onProjectEscaped(ProjectEscapedNotifier notifier) {
        wrapForCallbackInvocation(notifier, eventNotifier -> {
            String project = notifier.getProject();
            val kylinConfig = KylinConfig.getInstanceFromEnv();
            if (!GLOBAL.equals(project)) {
                log.info("Shutdown related thread: {}", project);
                try {
                    QueryHistoryMetaUpdateScheduler.shutdownByProject(project);
                    StreamingScheduler.shutdownByProject(project);
                } catch (Exception e) {
                    log.warn("error when shutdown " + project + " thread", e);
                }
            }
        });
    }

    private void wrapForCallbackInvocation(SchedulerEventNotifier notifier, Consumer<SchedulerEventNotifier> consumer) {
        try {
            consumer.accept(notifier);
        } finally {
            notifier.invokeCallbackIfExists();
        }
    }

    private void doOnGlobalControlled() {
        //TODO need global leader
        try {
            CreateAdminUserUtils.createAllAdmins(userService, env);
        } catch (IOException e) {
            throw new KylinRuntimeException(e);
        }
        InitUserGroupUtils.initUserGroups(env);
        UnitOfWork.doInTransactionWithRetry(() -> {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).createMetaStoreUuidIfNotExist();
            return null;
        }, "", 1);
        InitResourceGroupUtils.initResourceGroup();
        userAclService.syncAdminUserAcl();
    }

    private void doOnProjectControlled(String project, KylinConfig kylinConfig) {
        if (!EpochManager.getInstance().checkEpochValid(project)) {
            log.warn("epoch:{} is invalid in project controlled", project);
            return;
        }

        log.info("start thread of project: {}", project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            if (kylinConfig.isJobNode() || kylinConfig.isDataLoadingNode()) {
                initSchedule(kylinConfig, project);
            }

            if (kylinConfig.getQueryHistoryAccelerateInterval() > 0) {
                QueryHistoryMetaUpdateScheduler qhMetaUpdateScheduler = QueryHistoryMetaUpdateScheduler
                        .getInstance(project);
                qhMetaUpdateScheduler.init();
                if (!qhMetaUpdateScheduler.hasStarted()) {
                    throw new RuntimeException(
                            "Query history accelerate scheduler for " + project + " has not been started");
                }
            }
            return 0;
        }, project, 1);
    }
}
