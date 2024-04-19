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

package org.apache.kylin.job.runners;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.ThreadUtils;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JobCheckUtil {

    private static final Logger logger = LoggerFactory.getLogger(JobCheckUtil.class);

    private static ScheduledExecutorService jobCheckThreadPool;

    synchronized private static ScheduledExecutorService getJobCheckThreadPool() {
        if (null == jobCheckThreadPool) {
            jobCheckThreadPool = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JobCheckThreadPool");
        }
        return jobCheckThreadPool;
    }

    public static void startQuotaStorageCheckRunner(QuotaStorageCheckRunner quotaStorageCheckRunner) {
        if (!KylinConfig.getInstanceFromEnv().isStorageQuotaEnabled()) {
            return;
        }
        int pollSecond = KylinConfig.getInstanceFromEnv().getSchedulerPollIntervalSecond();
        getJobCheckThreadPool().scheduleWithFixedDelay(quotaStorageCheckRunner, RandomUtils.nextInt(0, pollSecond),
                pollSecond, TimeUnit.SECONDS);
    }

    public static void startJobCheckRunner(JobCheckRunner jobCheckRunner) {
        int pollSecond = KylinConfig.getInstanceFromEnv().getSchedulerPollIntervalSecond();
        getJobCheckThreadPool().scheduleWithFixedDelay(jobCheckRunner, RandomUtils.nextInt(0, pollSecond),
                pollSecond, TimeUnit.SECONDS);
    }

    public static boolean stopJobIfStorageQuotaLimitReached(JobContext jobContext, JobInfo jobInfo,
                                                            AbstractJobExecutable jobExecutable) {
        return stopJobIfStorageQuotaLimitReached(jobContext, JobInfoUtil.deserializeExecutablePO(jobInfo),
                jobExecutable);
    }

    public static boolean stopJobIfStorageQuotaLimitReached(JobContext jobContext, ExecutablePO executablePO,
            AbstractJobExecutable jobExecutable) {
        if (!KylinConfig.getInstanceFromEnv().isStorageQuotaEnabled()) {
            return false;
        }
        String jobId = executablePO.getId();
        String project = jobExecutable.getProject();
        try {
            if (jobContext.isProjectReachQuotaLimit(project)) {
                ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).pauseJob(jobId);
                logger.info("Job {} paused due to no available storage quota.", jobId);
                logger.info("Please clean up low-efficient storage in time, "
                        + "increase the low-efficient storage threshold, "
                        + "or notify the administrator to increase the storage quota for this project.");
                return true;
            }
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} failed to pause", project, jobId, e);
        }
        return false;
    }

    public static boolean markSuicideJob(String jobId, JobContext jobContext) {
        try {
            return JobContextUtil.withTxAndRetry(() -> {
                JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
                String project = jobInfo.getProject();
                ExecutableManager executableManager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
                AbstractExecutable job = executableManager.fromPO(JobInfoUtil.deserializeExecutablePO(jobInfo));
                if (checkSuicide(job)) {
                    executableManager.suicideJob(jobId);
                    return true;
                }
                return false;
            });
        } catch (Exception e) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED]  job {} should be suicidal but discard failed", jobId, e);
        }
        return false;
    }

    public static boolean checkSuicide(AbstractExecutable job) {
        if (job.getStatus().isFinalState()) {
            return false;
        }
        return job.checkSuicide();
    }

}
