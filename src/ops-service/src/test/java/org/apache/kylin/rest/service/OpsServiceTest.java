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

import static io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;

import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.rest.MockClusterManager;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.JobInfoTool;
import org.apache.kylin.tool.MetadataTool;
import org.apache.kylin.tool.QueryHistoryOffsetTool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ClusterManager.class, JobInfoTool.class, MetadataTool.class, QueryHistoryOffsetTool.class})
@PowerMockIgnore({ "javax.net.ssl.*", "javax.management.*", "org.apache.hadoop.*", "javax.security.*", "javax.crypto.*",
        "javax.script.*" })
public class OpsServiceTest extends NLocalFileMetadataTestCase {
    public OpsService opsService;

    @Before
    public void init() {
        PowerMockito.mockStatic(ClusterManager.class);
        ClusterManager clusterManager = new MockClusterManager();
        PowerMockito.when(ClusterManager.getInstance()).thenReturn(clusterManager);
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        createTestMetadata();
        opsService = new OpsService();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
        OpsService.MetadataBackupOperator.runningTask.clear();
    }

    @Test
    public void testBackupAndRestoreMetadata() throws IOException {
        String project = "default";
        QueryHistoryIdOffsetManager offsetManager = QueryHistoryIdOffsetManager.getInstance(project);
        QueryHistoryIdOffset originOffset = offsetManager.get(ACCELERATE);
        originOffset.setOffset(999L);
        offsetManager.saveOrUpdate(originOffset);

        FavoriteRuleManager favoriteRuleManager = FavoriteRuleManager.getInstance(project);
        FavoriteRule.Condition cond = new FavoriteRule.Condition(null, "10");
        FavoriteRule originRule = new FavoriteRule(Collections.singletonList(cond), "count", true);
        favoriteRuleManager.createRule(originRule);

        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id");
        jobInfo.setProject(project);
        jobInfo.setUpdateTime(new Date());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        String outPutPath = opsService.backupMetadata(project);
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = opsService.getMetadataBackupList(project);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (outPutPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatu.SUCCEED) {
                    return true;
                }
            }
            return false;
        });

        FavoriteRule newRule = new FavoriteRule(Collections.singletonList(cond), "count", true);
        FavoriteRule.Condition cond2 = new FavoriteRule.Condition(null, "20");
        newRule.setConds(Collections.singletonList(cond2));
        favoriteRuleManager.updateRule(newRule);
        FavoriteRule currentRule = favoriteRuleManager.getByName("count");
        Assert.assertNotEquals(((FavoriteRule.Condition) originRule.getConds().get(0)).getRightThreshold(),
                ((FavoriteRule.Condition) currentRule.getConds().get(0)).getRightThreshold());

        QueryHistoryIdOffset offset = offsetManager.get(ACCELERATE);
        offset.setOffset(111L);
        offsetManager.saveOrUpdate(offset);
        QueryHistoryIdOffset currentOffset = offsetManager.get(ACCELERATE);
        Assert.assertNotEquals(originOffset.getOffset(), currentOffset.getOffset());

        ExecutablePO modifiedExecutable = new ExecutablePO();
        modifiedExecutable.setName("modified_job_executable");
        byte[] modifiedJobContent = JobInfoUtil.serializeExecutablePO(modifiedExecutable);
        jobInfo.setJobContent(modifiedJobContent);
        jobInfoMapper.updateByJobIdSelective(jobInfo);
        JobInfo currentJobInfo = jobInfoMapper.selectByJobId(jobInfo.getJobId());
        Assert.assertNotEquals(originExecutable.getName(),
                JobInfoUtil.deserializeExecutablePO(currentJobInfo).getName());

        opsService.doMetadataRestore(outPutPath, project, true);
        await().atMost(1, TimeUnit.MINUTES).until(() -> !opsService.hasMetadataRestoreRunning());

        currentRule = favoriteRuleManager.getByName("count");
        Assert.assertEquals(((FavoriteRule.Condition) originRule.getConds().get(0)).getRightThreshold(),
                ((FavoriteRule.Condition) currentRule.getConds().get(0)).getRightThreshold());
        currentOffset = offsetManager.get(ACCELERATE);
        Assert.assertEquals(originOffset.getOffset(), currentOffset.getOffset());
        currentJobInfo = jobInfoMapper.selectByJobId(jobInfo.getJobId());
        Assert.assertEquals(originExecutable.getName(), JobInfoUtil.deserializeExecutablePO(currentJobInfo).getName());
    }

    @Test
    public void deleteBackup() throws IOException {
        String backupPath = opsService.backupMetadata(null);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = opsService
                    .getMetadataBackupList(OpsService._GLOBAL);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatu.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        Assert.assertTrue(
                opsService.deleteMetadataBackup(Lists.newArrayList(backupPath), null).contains("delete succeed"));
        Assert.assertTrue(opsService
                .deleteMetadataBackup(Lists.newArrayList(getTestConfig().getHdfsWorkingDirectory() + "sss",
                        getTestConfig().getHdfsWorkingDirectory()), null)
                .contains("path not exist" + " : " + "can not delete path not in metadata backup dir"));
    }

    @Test
    public void testMetadataRestoreStatus() throws IOException {
        String backupPath = opsService.backupMetadata(OpsService._GLOBAL);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList =
                    opsService.getMetadataBackupList(OpsService._GLOBAL);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatu.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        String uuid = opsService.doMetadataRestore(backupPath, null, false);
        await().atMost(10, TimeUnit.MINUTES).until(() -> {
            MetadataRestoreTask.MetadataRestoreStatus status = opsService.getMetadataRestoreStatus(uuid,
                    OpsService._GLOBAL);
            return status != null && !status.equals(MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS);
        });
        Assert.assertEquals(MetadataRestoreTask.MetadataRestoreStatus.SUCCEED,
                opsService.getMetadataRestoreStatus(uuid, OpsService._GLOBAL));

        JobInfoTool jobInfoTool = Mockito.mock(JobInfoTool.class);
        Mockito.doThrow(new IOException()).when(jobInfoTool)
                .restoreProject(anyString(), anyString(), anyBoolean());
        OpsService.MetadataRestoreOperator operator =
                new OpsService.MetadataRestoreOperator(backupPath, OpsService._GLOBAL, false);
        operator.setJobInfoTool(jobInfoTool);
        String uuid2 = operator.submitMetadataRestore();

        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            MetadataRestoreTask.MetadataRestoreStatus status = opsService.getMetadataRestoreStatus(uuid2,
                    OpsService._GLOBAL);
            return status != null && !status.equals(MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS);
        });
        Assert.assertEquals(MetadataRestoreTask.MetadataRestoreStatus.FAILED,
                opsService.getMetadataRestoreStatus(uuid2, OpsService._GLOBAL));
    }

    @Test
    public void testCancelAndDeleteMetadataBackup() throws IOException, InterruptedException, ExecutionException {
        String backupPath = opsService.backupMetadata(OpsService._GLOBAL);
        JobContextUtil.getJobContext(getTestConfig());
        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id3");
        jobInfo.setProject(OpsService._GLOBAL);
        jobInfo.setUpdateTime(new Date());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        Assert.assertTrue(HadoopUtil.getWorkingFileSystem().exists(new Path(backupPath)));
        opsService.cancelAndDeleteMetadataBackup(backupPath, OpsService._GLOBAL);
        Assert.assertEquals(OpsService.MetadataBackupStatu.CANCELED,
                OpsService.getMetadataBackupList(OpsService._GLOBAL)
                        .stream()
                        .filter(backup -> backup.getPath().equals(backupPath))
                        .findFirst()
                        .get().getStatus());
        await().atMost(10, TimeUnit.SECONDS).until(
                () -> !HadoopUtil.getWorkingFileSystem().exists(new Path(backupPath)));
    }

    @Test
    public void testBackupMetadataFailed() throws Exception {
        QueryHistoryOffsetTool queryHistoryOffsetTool = Mockito.mock(QueryHistoryOffsetTool.class);
        Mockito.doThrow(new IOException()).when(queryHistoryOffsetTool).backup(anyString(), anyString());
        OpsService.MetadataBackupOperator metadataBackupOperator = new OpsService.MetadataBackupOperator(
                OpsService.getMetaBackupStoreDir(OpsService._GLOBAL),
                HadoopUtil.getWorkingFileSystem(), "test_failed", OpsService._GLOBAL);
        metadataBackupOperator.setQueryHistoryOffsetTool(queryHistoryOffsetTool);
        String backupPath = metadataBackupOperator.startBackup();
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = OpsService
                    .getMetadataBackupList(OpsService._GLOBAL);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (backupPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatu.FAILED) {
                    return true;
                }
            }
            return false;
        });
        Assert.assertEquals(OpsService.MetadataBackupStatu.FAILED,
                OpsService.getMetadataBackupList(OpsService._GLOBAL).get(0).getStatus());
    }

    @Test
    public void testMetadataBackupStatusDirtyRead() throws IOException, ExecutionException, InterruptedException {
        FileSystem fileSystem = HadoopUtil.getWorkingFileSystem();
        String username = AclPermissionUtil.getCurrentUsername();
        // to avoid JsonUtil class initialize take too long time
        new JsonUtil();
        MockedMetadataRestoreOperator operator1 = new MockedMetadataRestoreOperator(
                OpsService.GLOBAL_METADATA_BACKUP_PATH, fileSystem,
                username, OpsService._GLOBAL);
        JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv());
        String path1 = operator1.startBackup();

        List<MetadataBackupResponse> projectMetadataBackupList1 = opsService.getMetadataBackupList(OpsService._GLOBAL)
                .stream().filter(response -> response.getPath().equals(path1)).collect(Collectors.toList());
        // default retry get in progress status
        Assert.assertEquals(1, projectMetadataBackupList1.size());
        Assert.assertEquals(OpsService.MetadataBackupStatu.IN_PROGRESS, projectMetadataBackupList1.get(0).getStatus());
        opsService.cancelAndDeleteMetadataBackup(path1, OpsService._GLOBAL);

        // for retry time is zero, status write time takes too long, will get an unknown status.
        OpsService.defaultStatusReadRetryTime = 0;
        MockedMetadataRestoreOperator operator2 = new MockedMetadataRestoreOperator(
                OpsService.GLOBAL_METADATA_BACKUP_PATH, fileSystem,
                username, OpsService._GLOBAL);
        String path2 = operator2.startBackup();
        List<MetadataBackupResponse> projectMetadataBackupList2 = opsService.getMetadataBackupList(OpsService._GLOBAL)
                .stream().filter(response -> response.getPath().equals(path2)).collect(Collectors.toList());

        // default retry get in progress status
        Assert.assertEquals(1, projectMetadataBackupList2.size());
        Assert.assertEquals(OpsService.MetadataBackupStatu.UNKNOWN, projectMetadataBackupList2.get(0).getStatus());
    }

    @Test
    public void testMetadataRestoreFailed() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String project = "metadata_restore_failed_test";
        NProjectManager projectManager = NProjectManager.getInstance(config);
        projectManager.createProject(project, AclPermissionUtil.getCurrentUsername(), null, null);
        JobInfoMapper jobInfoMapper = JobContextUtil.getJobContext(KylinConfig.getInstanceFromEnv()).getJobInfoMapper();
        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id2");
        jobInfo.setProject(project);
        jobInfo.setUpdateTime(new Date());
        ExecutablePO originExecutable = new ExecutablePO();
        originExecutable.setName("origin_executable");
        byte[] originJobContent = JobInfoUtil.serializeExecutablePO(originExecutable);
        jobInfo.setJobContent(originJobContent);
        jobInfoMapper.insert(jobInfo);

        String outPutPath = opsService.backupMetadata(project);
        await().atMost(1, TimeUnit.MINUTES).until(() -> {
            List<MetadataBackupResponse> projectMetadataBackupList = opsService.getMetadataBackupList(project);
            for (MetadataBackupResponse response : projectMetadataBackupList) {
                if (outPutPath.equals(response.getPath())
                        && response.getStatus() == OpsService.MetadataBackupStatu.SUCCEED) {
                    return true;
                }
            }
            return false;
        });
        projectManager.dropProject(project);
        jobInfoMapper.deleteByJobId(jobInfo.getJobId());
        Assert.assertEquals(projectManager.getProject(project), null);
        Assert.assertEquals(jobInfoMapper.selectByJobId(jobInfo.getJobId()), null);

        opsService.doMetadataRestore(outPutPath, project, false);
        await().atMost(1, TimeUnit.MINUTES).until(() -> !opsService.hasMetadataRestoreRunning());
        projectManager.reloadAll();
        Assert.assertNotEquals(projectManager.getProject(project), null);
        Assert.assertNotEquals(jobInfoMapper.selectByJobId(jobInfo.getJobId()), null);
        Thread.sleep(1000);

        projectManager.dropProject(project);
        jobInfoMapper.deleteByJobId(jobInfo.getJobId());
        JobInfoTool jobInfoTool = Mockito.mock(JobInfoTool.class);
        Mockito.doThrow(new IOException()).when(jobInfoTool).restoreProject(anyString(), anyString(), anyBoolean());
        OpsService.MetadataRestoreOperator operator =
                new OpsService.MetadataRestoreOperator(outPutPath, project, false);
        operator.setJobInfoTool(jobInfoTool);
        operator.submitMetadataRestore();
        await().atMost(1, TimeUnit.MINUTES).until(() -> !opsService.hasMetadataRestoreRunning());
        projectManager.reloadAll();
        Assert.assertEquals(jobInfoMapper.selectByJobId(jobInfo.getJobId()), null);
        Assert.assertEquals(projectManager.getProject(project), null);
    }

    private class MockedMetadataRestoreOperator extends OpsService.MetadataBackupOperator {

        public MockedMetadataRestoreOperator(String dir, FileSystem fs, String user, String project) {
            super(dir, fs, user, project);
        }

        @Override
        public void updateStatus() throws IOException {
            try (FSDataOutputStream fos = getFs().create(getStatusPath(), true)) {
                String value = JsonUtil.writeValueAsString(getStatuInfo());
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                fos.writeUTF(value);
            }
        }
    }
}