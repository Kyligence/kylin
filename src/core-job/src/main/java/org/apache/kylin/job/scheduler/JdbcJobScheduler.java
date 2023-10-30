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

package org.apache.kylin.job.scheduler;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.constant.LogConstant;
import org.apache.kylin.common.logging.SetLogCategory;
import org.apache.kylin.common.util.ExecutorServiceUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ThreadUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.core.AbstractJobExecutable;
import org.apache.kylin.job.core.lock.JdbcJobLock;
import org.apache.kylin.job.core.lock.LockAcquireListener;
import org.apache.kylin.job.core.lock.LockException;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.domain.JobLock;
import org.apache.kylin.job.domain.PriorityFistRandomOrderJob;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.job.runners.JobCheckUtil;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.apache.kylin.rest.util.SpringContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;

import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.zen.license.check.CheckResult;
import io.kyligence.zen.license.check.LicenseChecker;
import io.kyligence.zen.license.check.ResultType;
import lombok.Getter;
import lombok.val;

public class JdbcJobScheduler implements JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(LogConstant.SCHEDULE_CATEGORY);

    private final JobContext jobContext;

    private final AtomicBoolean isMaster;

    // job id -> (executable, job scheduled time)
    private final Map<String, Pair<AbstractJobExecutable, Long>> runningJobMap;

    private JdbcJobLock masterLock;

    private ScheduledExecutorService master;

    private ScheduledExecutorService slave;

    private ThreadPoolExecutor executorPool;

    private int consumerMaxThreads;

    public JdbcJobScheduler(JobContext jobContext) {
        this.jobContext = jobContext;
        this.isMaster = new AtomicBoolean(false);
        this.runningJobMap = Maps.newConcurrentMap();
        this.consumerMaxThreads = jobContext.getKylinConfig().getMaxConcurrentJobLimit();
    }

    @Override
    public void publishJob() {
        // master lock
        masterLock = new JdbcJobLock(JobScheduler.MASTER_SCHEDULER, jobContext.getServerNode(),
                jobContext.getKylinConfig().getJobSchedulerMasterRenewalSec(),
                jobContext.getKylinConfig().getJobSchedulerMasterRenewalRatio(), jobContext.getLockClient(),
                new MasterAcquireListener());
        // standby: acquire master lock
        master.schedule(this::standby, 0, TimeUnit.SECONDS);

        // master: publish job
        master.schedule(this::produceJob, 0, TimeUnit.SECONDS);
    }

    @Override
    public void subscribeJob() {
        // slave: subscribe job
        slave.schedule(this::consumeJob, 0, TimeUnit.SECONDS);
    }

    // public for UT
    public boolean hasRunningJob() {
        return !runningJobMap.isEmpty();
    }

    public Map<String, Pair<AbstractJobExecutable, Long>> getRunningJob() {
        return Collections.unmodifiableMap(runningJobMap);
    }

    @Override
    public String getJobNode(String jobId) {
        return JobContextUtil.getJobSchedulerHost(jobId);
    }

    public void start() {
        // standby: acquire JSM
        // publish job:  READY -> PENDING
        master = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Master");

        // subscribe job: PENDING -> RUNNING
        slave = ThreadUtils.newDaemonSingleThreadScheduledExecutor("JdbcJobScheduler-Slave");

        int maxThreads = this.consumerMaxThreads <= 0 ? 1 : this.consumerMaxThreads;
        // execute job: RUNNING -> FINISHED
        executorPool = ThreadUtils.newDaemonScalableThreadPool("JdbcJobScheduler-Executor", 1, maxThreads, 5,
                TimeUnit.MINUTES);

        publishJob();
        subscribeJob();
    }

    public void destroy() {

        if (Objects.nonNull(masterLock)) {
            try {
                masterLock.tryRelease();
            } catch (LockException e) {
                logger.error("Something's wrong when removing master lock", e);
            }
        }

        if (Objects.nonNull(master)) {
            master.shutdownNow();
        }

        if (Objects.nonNull(slave)) {
            slave.shutdownNow();
        }

        if (Objects.nonNull(executorPool)) {
            ExecutorServiceUtil.shutdownGracefully(executorPool, 60);
        }
    }

    private void standby() {
        // init master lock
        try {
            if (jobContext.getJobLockMapper().selectByJobId(JobScheduler.MASTER_SCHEDULER) == null) {
                jobContext.getJobLockMapper().insertSelective(new JobLock(JobScheduler.MASTER_SCHEDULER, "_global", 0));
            }
        } catch (Exception e) {
            logger.error("Try insert 'master_scheduler' failed.", e);
        }

        try {
            masterLock.tryAcquire();
        } catch (LockException e) {
            logger.error("Something's wrong when acquiring master lock.", e);
        }
    }

    private void produceJob() {
        long delaySec = jobContext.getKylinConfig().getJobSchedulerMasterPollIntervalSec();
        try {
            // only master can publish job
            if (!isMaster.get()) {
                return;
            }

            releaseExpiredLock();

            // parallel job count threshold
            if (!jobContext.getParallelLimiter().tryRelease()) {
                return;
            }

            int batchSize = jobContext.getKylinConfig().getJobSchedulerMasterPollBatchSize();
            List<String> readyJobIdList = jobContext.getJobInfoMapper()
                    .findJobIdListByStatusBatch(ExecutableState.READY.name(), batchSize);
            if (readyJobIdList.isEmpty()) {
                return;
            }

            if (!checkLicense()) {
                return;
            }

            String polledJobIdInfo = readyJobIdList.stream().collect(Collectors.joining(",", "[", "]"));
            logger.info("Scheduler polled jobs: {} {}", readyJobIdList.size(), polledJobIdInfo);
            // force catchup metadata before produce jobs
            StreamingUtils.replayAuditlog();
            for (String jobId : readyJobIdList) {
                if (!jobContext.getParallelLimiter().tryAcquire()) {
                    return;
                }

                if (JobCheckUtil.markSuicideJob(jobId, jobContext)) {
                    logger.info("suicide job = {} on produce", jobId);
                    continue;
                }

                JobContextUtil.withTxAndRetry(() -> {
                    JobLock lock = jobContext.getJobLockMapper().selectByJobId(jobId);
                    JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
                    if (lock == null && jobContext.getJobLockMapper()
                            .insertSelective(new JobLock(jobId, jobInfo.getProject(), jobInfo.getPriority())) == 0) {
                        logger.error("Create job lock for [{}] failed!", jobId);
                        return null;
                    }
                    ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                            .publishJob(jobId, (AbstractExecutable) getJobExecutable(jobInfo));
                    return null;
                });
            }

            // maybe more jobs exist, publish job immediately
            delaySec = 0;
        } catch (Exception e) {
            logger.error("Something's wrong when publishing job", e);
        } finally {
            master.schedule(this::produceJob, delaySec, TimeUnit.SECONDS);
        }
    }

    private boolean checkLicense() {
        LicenseChecker licenseChecker = getLicenseChecker();
        if (null == licenseChecker) {
            // skip license check
            return true;
        }
        CheckResult checkResult = null;
        try {
            checkResult = licenseChecker.check();
        } catch (Throwable e) {
            logger.error("Error happened when check license", e);
            return false;
        }
        if (checkResult.getResultType() == ResultType.VALID) {
            return true;
        }
        logger.error("License check failed: {}", checkResult.getMessage());
        return false;
    }

    private LicenseChecker getLicenseChecker() {
        try {
            if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
                return null;
            }
            return SpringContext.getBean(LicenseChecker.class);
        } catch (NoSuchBeanDefinitionException e) {
            logger.debug("LicenseChecker can not be found.");
            return null;
        }
    }

    private void releaseExpiredLock() {
        int batchSize = jobContext.getKylinConfig().getJobSchedulerMasterPollBatchSize();
        JobMapperFilter filter = new JobMapperFilter();
        List<String> jobIds = jobContext.getJobLockMapper().findExpiredORNonLockIdList(batchSize);
        if (jobIds.isEmpty()) {
            return;
        }
        filter.setJobIds(jobIds);
        List<JobInfo> jobs = jobContext.getJobInfoMapper().selectByJobFilter(filter);
        List<String> jobInfoIds = jobs.stream().map(JobInfo::getJobId).collect(Collectors.toList());
        List<String> toRemoveLocks = Lists.newArrayList(jobIds).stream().filter(jobId -> !jobInfoIds.contains(jobId))
                .collect(Collectors.toList());
        for (JobInfo job : jobs) {
            ExecutablePO po = JobInfoUtil.deserializeExecutablePO(job);
            if (po != null) {
                ExecutableState state = ExecutableState.valueOf(po.getOutput().getStatus());
                if (state.isNotProgressing() || state.isFinalState()) {
                    toRemoveLocks.add(job.getJobId());
                }
            }
        }
        if (!toRemoveLocks.isEmpty()) {
            jobContext.getJobLockMapper().batchRemoveLock(toRemoveLocks);
        }
    }

    private void consumeJob() {
        long delay = jobContext.getKylinConfig().getSchedulerPollIntervalSecond();
        try {
            // The number of tasks to be obtained cannot exceed the free slots of the 'executorPool'
            int exeFreeSlots = this.consumerMaxThreads - this.runningJobMap.size();
            if (exeFreeSlots <= 0) {
                logger.info("No free slots to execute job");
                return;
            }
            int batchSize = jobContext.getKylinConfig().getJobSchedulerSlavePollBatchSize();
            if (exeFreeSlots < batchSize) {
                batchSize = exeFreeSlots;
            }
            List<String> projects = EpochManager.getInstance().listProjectWithPermissionForScheduler();
            List<String> jobIdList = findNonLockIdListInOrder(batchSize, projects);

            if (CollectionUtils.isEmpty(jobIdList)) {
                return;
            }
            // for slave node, force catchup metadata before execute jobs
            if (!isMaster.get()) {
                StreamingUtils.replayAuditlog();
            }
            // submit job
            jobIdList.forEach(jobId -> {
                // check 'runningJobMap', avoid processing submitted tasks
                if (runningJobMap.containsKey(jobId)) {
                    logger.warn("Job {} has already been submitted", jobId);
                    return;
                }
                Pair<JobInfo, AbstractJobExecutable> job = fetchJob(jobId);
                if (null == job) {
                    return;
                }
                final JobInfo jobInfo = job.getFirst();
                final AbstractJobExecutable jobExecutable = job.getSecond();
                if (!canSubmitJob(jobId, jobInfo, jobExecutable)) {
                    return;
                }
                // record running job id, and job scheduled time
                runningJobMap.put(jobId, new Pair<>(jobExecutable, System.currentTimeMillis()));
                executorPool.execute(() -> executeJob(jobExecutable, jobInfo));
            });
        } catch (Exception e) {
            logger.error("Something's wrong when consuming job", e);
        } finally {
            logger.info("{} running jobs in current scheduler", getRunningJob().size());
            slave.schedule(this::consumeJob, delay, TimeUnit.SECONDS);
        }
    }

    public List<String> findNonLockIdListInOrder(int batchSize, List<String> projects) {
        List<PriorityFistRandomOrderJob> jobIdList = jobContext.getJobLockMapper().findNonLockIdList(batchSize,
                projects);
        // Shuffle jobs avoiding jobLock conflict.
        // At the same time, we should ensure the overall order.
        if (hasRunningJob()) {
            Collections.sort(jobIdList);
        }
        return jobIdList.stream().map(PriorityFistRandomOrderJob::getJobId).collect(Collectors.toList());
    }

    private Pair<JobInfo, AbstractJobExecutable> fetchJob(String jobId) {
        try {
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            if (jobInfo == null) {
                logger.warn("can not find job info {}", jobId);
                return null;
            }
            AbstractJobExecutable jobExecutable = getJobExecutable(jobInfo);
            return new Pair<>(jobInfo, jobExecutable);
        } catch (Throwable throwable) {
            logger.error("Fetch job failed, job id: " + jobId, throwable);
            return null;
        }
    }

    private boolean canSubmitJob(String jobId, JobInfo jobInfo, AbstractJobExecutable jobExecutable) {
        try {
            if (JobScheduler.MASTER_SCHEDULER.equals(jobId)) {
                return false;
            }
            if (Objects.isNull(jobInfo)) {
                logger.warn("Job not found: {}", jobId);
                return false;
            }
            if (!jobContext.getResourceAcquirer().tryAcquire(jobExecutable)) {
                return false;
            }
        } catch (Throwable throwable) {
            logger.error("Error when preparing to submit job: " + jobId, throwable);
            return false;
        }
        return true;
    }

    private void executeJob(AbstractJobExecutable jobExecutable, JobInfo jobInfo) {
        JdbcJobLock jobLock = null;
        try (JobExecutor jobExecutor = new JobExecutor(jobContext, jobExecutable);
                SetLogCategory ignore = new SetLogCategory(LogConstant.BUILD_CATEGORY)) {
            // Check job status
            AbstractExecutable executable = (AbstractExecutable) jobExecutable;
            ExecutableState jobStatus = executable.getStatus();
            if (ExecutableState.PENDING != jobStatus) {
                logger.warn("Unexpected status for {} <{}>, should not execute job", jobExecutable.getJobId(),
                        jobStatus);
                if (ExecutableState.RUNNING == jobStatus) {
                    jobLock = tryJobLock(jobExecutable);
                    if (jobLock != null) {
                        //Maybe other node crashed during job execution, resume job status from running to ready.
                        logger.warn("Resume <RUNNING> job {}", jobExecutable.getJobId());
                        ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), executable.getProject())
                                .resumeJob(jobExecutable.getJobId(), true);
                    }
                }
                return;
            }

            jobLock = tryJobLock(jobExecutable);
            if (null == jobLock) {
                return;
            }
            if (jobContext.isProjectReachQuotaLimit(jobExecutable.getProject())
                    && JobCheckUtil.stopJobIfStorageQuotaLimitReached(jobContext, jobInfo, jobExecutable)) {
                return;
            }
            // heavy action
            jobExecutor.execute();
        } catch (Throwable t) {
            logger.error("Execute job failed " + jobExecutable.getJobId(), t);
        } finally {
            if (jobLock != null) {
                stopJobLockRenewAfterExecute(jobLock);
            }
            runningJobMap.remove(jobExecutable.getJobId());
        }
    }

    private JdbcJobLock tryJobLock(AbstractJobExecutable jobExecutable) throws LockException {
        JdbcJobLock jobLock = new JdbcJobLock(jobExecutable.getJobId(), jobContext.getServerNode(),
                jobContext.getKylinConfig().getJobSchedulerJobRenewalSec(),
                jobContext.getKylinConfig().getJobSchedulerJobRenewalRatio(), jobContext.getLockClient(),
                new JobAcquireListener(jobExecutable));
        if (!jobLock.tryAcquire()) {
            logger.info("Acquire job lock failed.");
            return null;
        }
        return jobLock;
    }

    private void stopJobLockRenewAfterExecute(JdbcJobLock jobLock) {
        try {
            String jobId = jobLock.getLockId();
            JobInfo jobInfo = jobContext.getJobInfoMapper().selectByJobId(jobId);
            AbstractExecutable jobExecutable = (AbstractExecutable) getJobExecutable(jobInfo);
            if (jobExecutable.getStatusInMem() == ExecutableState.RUNNING) {
                logger.error("Unexpected status for {} <{}>, mark job error", jobId, jobExecutable.getStatusInMem());
                markErrorJob(jobId, jobExecutable.getProject());
            }
        } catch (Throwable t) {
            logger.error("Fail to check status before stop renew job lock {}", jobLock.getLockId(), t);
        } finally {
            jobLock.stopRenew();
        }
    }

    private void markErrorJob(String jobId, String project) {
        try {
            val manager = ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            manager.errorJob(jobId);
        } catch (Throwable t) {
            logger.warn("[UNEXPECTED_THINGS_HAPPENED] project {} job {} should be error but mark failed", project,
                    jobId, t);
            throw t;
        }
    }

    private AbstractJobExecutable getJobExecutable(JobInfo jobInfo) {
        ExecutablePO executablePO = JobInfoUtil.deserializeExecutablePO(jobInfo);
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), jobInfo.getProject())
                .fromPO(executablePO);
    }

    private class MasterAcquireListener implements LockAcquireListener {

        @Override
        public void onSucceed() {
            if (isMaster.compareAndSet(false, true)) {
                logger.info("Job scheduler become master.");
            } else {
                logger.debug("Job scheduler keep on master");
            }
        }

        @Override
        public void onFailed() {
            if (isMaster.compareAndSet(true, false)) {
                logger.info("Job scheduler fallback standby.");
            } else {
                logger.debug("Job scheduler keep on standby");
            }
            // standby
            master.schedule(JdbcJobScheduler.this::standby, masterLock.getRenewalSec(), TimeUnit.SECONDS);
        }
    }

    private static class JobAcquireListener implements LockAcquireListener {

        @Getter
        private final AbstractJobExecutable jobExecutable;

        JobAcquireListener(AbstractJobExecutable jobExecutable) {
            this.jobExecutable = jobExecutable;
        }

        @Override
        public void onSucceed() {
            // do nothing
        }

        @Override
        public void onFailed() {
            // do nothing
        }
    }

}
