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

import static org.apache.kylin.common.exception.code.ErrorCodeTool.METADATA_BACKUP_IS_IN_PROGRESS;
import static org.apache.kylin.common.exception.code.ErrorCodeTool.SYSTEM_IN_METADATA_RECOVER;
import static org.apache.kylin.common.persistence.transaction.UnitOfWork.GLOBAL_UNIT;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.helper.MetadataToolHelper;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.metadata.asynctask.MetadataRestoreTask;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.reponse.MetadataBackupResponse;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.apache.kylin.tool.CancelHook;
import org.apache.kylin.tool.FavoriteRuleTool;
import org.apache.kylin.tool.JobInfoTool;
import org.apache.kylin.tool.MaintainModeTool;
import org.apache.kylin.tool.QueryHistoryOffsetTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import io.kyligence.kap.metadata.favorite.AsyncTaskManager;

@Service("opsService")
public class OpsService {

    public static final Logger log = LoggerFactory.getLogger(OpsService.class);

    public static final String META_BACKUP_DIR = "_metadata_backup";
    public static final String SYSTEM_LEVEL_METADATA_BACKUP_DIR_NAME = "_system_level_metadata_backup";
    public static final String PROJECT_LEVEL_METADATA_BACKUP_DIR_NAME = "_project_level_metadata_backup";
    public static final String BACKUP_STATUS = "_backup_status";
    public static final String _GLOBAL = GLOBAL_UNIT;
    public static final String CORE_META_DIR = "core_meta";
    public static final String META_BACKUP_PATH = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory()
            + META_BACKUP_DIR;
    public static final String GLOBAL_METADATA_BACKUP_PATH = META_BACKUP_PATH + "/"
            + SYSTEM_LEVEL_METADATA_BACKUP_DIR_NAME;
    public static final String PROJECT_METADATA_BACKUP_PATH = META_BACKUP_PATH + "/"
            + PROJECT_LEVEL_METADATA_BACKUP_DIR_NAME;
    public static int defaultStatusReadRetryTime = 5;

    public static String getMetaBackupStoreDir(String project) {
        if (project == null || project.equals(_GLOBAL)) {
            return GLOBAL_METADATA_BACKUP_PATH;
        } else {
            return PROJECT_METADATA_BACKUP_PATH + "/" + project;
        }
    }

    public String backupMetadata(String project) {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String username = AclPermissionUtil.getCurrentUsername();
        MetadataBackupOperator metadataBackupOperator = new MetadataBackupOperator(getMetaBackupStoreDir(project), fs,
                username, project);
        return metadataBackupOperator.startBackup();
    }

    public static List<MetadataBackupResponse> getMetadataBackupList(String project) throws IOException {
        String pathStr = getMetaBackupStoreDir(project);
        Path path = new Path(pathStr);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        List<MetadataBackupResponse> res = Lists.newArrayList();
        if (!fs.exists(path)) {
            return res;
        }
        for (FileStatus fileStatu : fs.listStatus(path)) {
            if (fileStatu.isDirectory()) {
                Path statusPath = new Path(fileStatu.getPath().toString() + "/" + BACKUP_STATUS);
                MetadataBackupResponse response = new MetadataBackupResponse();
                response.setPath(fileStatu.getPath().toUri().toString());
                if (fs.exists(statusPath)) {
                    MetadataBackupStatuInfo backupStatuInfo = getMetadataStatusInfoWithRetry(statusPath, fs,
                            defaultStatusReadRetryTime);
                    response.setStatus(backupStatuInfo.getStatus());
                    response.setSize(backupStatuInfo.getSize());
                    response.setOwner(backupStatuInfo.getOwner());
                }
                res.add(response);
            }
        }
        return res;
    }

    public static MetadataBackupStatuInfo getMetadataStatusInfoWithRetry(Path statusPath, FileSystem fs,
            int retryTime) {
        MetadataBackupStatuInfo metadataBackupStatuInfo = new MetadataBackupStatuInfo();
        do {
            try (FSDataInputStream fis = fs.open(statusPath)) {
                String value = fis.readUTF();
                metadataBackupStatuInfo = JsonUtil.readValue(value, MetadataBackupStatuInfo.class);
                return metadataBackupStatuInfo;
            } catch (Exception e) {
                retryTime--;
                try {
                    Thread.sleep(200);
                } catch (InterruptedException ex) {
                    log.error(e.getMessage(), e);
                    Thread.currentThread().interrupt();
                }
                log.error(e.getMessage(), e);
                metadataBackupStatuInfo.setStatus(MetadataBackupStatu.UNKNOWN);
            }
        } while (retryTime >= 0);
        return metadataBackupStatuInfo;
    }

    public static String deleteMetadataBackup(List<String> pathStrs, String project) throws IOException {
        StringBuilder sb = new StringBuilder();
        for (String pathStr : pathStrs) {
            String response = deleteMetadataBackup(pathStr, project);
            if (sb.length() == 0) {
                sb.append(response);
            } else {
                sb.append(" : ");
                sb.append(response);
            }
        }
        return sb.toString();
    }

    public static String deleteMetadataBackup(String pathStr, String project) throws IOException {
        if (pathStr != null) {
            Path path = new Path(pathStr);
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            if (fs.exists(path)) {
                if (!pathStr.startsWith(META_BACKUP_PATH)) {
                    return "can not delete path not in metadata backup dir " + META_BACKUP_PATH;
                }
                String rootPath = getMetaBackupStoreDir(project);
                if (!pathStr.startsWith(rootPath) || pathStr.equals(rootPath)) {
                    return "can not delete path not in metadata backup dir " + rootPath;
                }
                fs.delete(path, true);
                log.info("Delete metadata backup {} succeed.", path.toUri());
                if (fs.listStatus(path.getParent()).length == 0) {
                    fs.delete(path.getParent(), true);
                    log.info("Delete project path {} for no metadata backup exist.", path.getParent().toUri());
                }
                return path + " delete succeed.";
            }
        }
        return pathStr + " path not exist";
    }

    public void cancelAndDeleteMetadataBackup(String rootPath, String project) throws IOException {
        MetadataBackupOperator.cancelAndAsyncDeleteBackup(rootPath, project);
    }

    public String doMetadataRestore(String path, String project, boolean afterTruncate) {
        if (project == null) {
            project = _GLOBAL;
        }
        MetadataRestoreOperator operator = new MetadataRestoreOperator(path, project, afterTruncate);
        return operator.submitMetadataRestore();
    }

    public MetadataRestoreTask.MetadataRestoreStatus getMetadataRestoreStatus(String uuid, String project) {
        if (uuid == null) {
            return null;
        }
        AsyncTaskManager taskManager = AsyncTaskManager.getInstance(project);
        AbstractAsyncTask asyncTask = taskManager.get(AsyncTaskManager.METADATA_RECOVER_TASK, uuid);
        if (asyncTask == null) {
            return null;
        } else {
            return ((MetadataRestoreTask) asyncTask).getStatus();
        }
    }

    public boolean hasMetadataRestoreRunning() {
        return MetadataRestoreOperator.hasMetadataRestoreRunning();
    }

    public static class MetadataBackupOperator {
        public static final Logger log = LoggerFactory.getLogger(MetadataBackupOperator.class);
        public static final Map<String, Pair<Future, MetadataBackupOperator>> runningTask = new ConcurrentHashMap<>();
        private static final ExecutorService executor = Executors.newCachedThreadPool();

        private FileSystem fs;
        private String rootPath;
        private Path statusPath;
        private MetadataBackupStatuInfo statuInfo;
        private String project;
        private String projectMetadataBackupKey;

        private MetadataToolHelper coreMetadataTool;
        private JobInfoTool jobInfoTool;
        private QueryHistoryOffsetTool queryHistoryOffsetTool;
        private FavoriteRuleTool favoriteRuleTool;

        public MetadataBackupOperator(String dir, FileSystem fs, String user, String project) {
            this.projectMetadataBackupKey = dir;
            this.rootPath = dir + "/" + System.currentTimeMillis();
            this.fs = fs;
            this.statuInfo = new MetadataBackupStatuInfo();
            this.statusPath = new Path(rootPath + "/" + BACKUP_STATUS);
            this.project = project;
            statuInfo.setOwner(user);
            statuInfo.setPath(rootPath);
            init();
        }

        public MetadataBackupOperator(MetadataBackupResponse response, String project) {
            this.rootPath = response.getPath();
            this.project = project;
            this.fs = HadoopUtil.getWorkingFileSystem();
            this.statusPath = new Path(rootPath + "/" + BACKUP_STATUS);
            this.statuInfo = new MetadataBackupStatuInfo();
            statuInfo.setOwner(response.getOwner());
            statuInfo.setPath(rootPath);
            init();
        }

        private void init() {
            CancelHook hook = () -> runningTask.containsKey(projectMetadataBackupKey);
            this.coreMetadataTool = new MetadataToolHelper();
            coreMetadataTool.setHook(hook);
            this.queryHistoryOffsetTool = new QueryHistoryOffsetTool();
            queryHistoryOffsetTool.setHook(hook);
            this.favoriteRuleTool = new FavoriteRuleTool();
            favoriteRuleTool.setHook(hook);
            this.jobInfoTool = new JobInfoTool();
            jobInfoTool.setHook(hook);
            if (project == null || project.equals(_GLOBAL)) {
                List<String> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects()
                        .stream().map(ProjectInstance::getName).collect(Collectors.toList());
                statuInfo.setProjects(projects);
            } else {
                statuInfo.setProjects(Lists.newArrayList(project));
            }
        }

        public static synchronized void cancelAndAsyncDeleteBackup(String rootPath, String project) throws IOException {
            String key = getProjectMetadataKeyFromRootPath(rootPath);
            if (runningTask.containsKey(key)) {
                Future future = runningTask.get(key).getFirst();
                cancelBackup(key);
                executor.submit(() -> {
                    try {
                        future.get();
                        OpsService.deleteMetadataBackup(rootPath, project);
                    } catch (Throwable e) {
                        log.error(e.getMessage(), e);
                    }
                });
            }
        }

        public static void cancelBackup(String projectMetadataBackupKey) throws IOException {
            Pair<Future, MetadataBackupOperator> pair = runningTask.remove(projectMetadataBackupKey);
            pair.getSecond().markCanceled();
        }

        public static String getProjectMetadataKeyFromRootPath(String rootPath) {
            return rootPath.substring(0, rootPath.lastIndexOf('/'));
        }

        public void doBackupMetadata() throws Exception {
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            if (project == null || project.equals(_GLOBAL)) {
                List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                        .listAllProjects();
                coreMetadataTool.backupToDirectPath(config, rootPath + "/" + CORE_META_DIR);
                for (ProjectInstance projectInstance : projects) {
                    queryHistoryOffsetTool.backup(rootPath, projectInstance.getName());
                    favoriteRuleTool.backup(rootPath, projectInstance.getName());
                    jobInfoTool.backup(rootPath, projectInstance.getName());
                }
            } else {
                coreMetadataTool.backupToDirectPath(config, rootPath + "/" + CORE_META_DIR, project);
                queryHistoryOffsetTool.backup(rootPath, project);
                favoriteRuleTool.backup(rootPath, project);
                jobInfoTool.backup(rootPath, project);
            }
        }

        public long getSize() throws IOException {
            long size = 0L;
            for (FileStatus status : fs.listStatus(new Path(rootPath))) {
                if (status.isDirectory()) {
                    for (FileStatus subStatus : fs.listStatus(status.getPath())) {
                        size += subStatus.getLen();
                    }
                }
                size += status.getLen();
            }
            return size;
        }

        public void updateStatus() throws IOException {
            try (FSDataOutputStream fos = fs.create(statusPath, true)) {
                String value = JsonUtil.writeValueAsString(statuInfo);
                fos.writeUTF(value);
            }
        }

        public void markCanceled() throws IOException {
            statuInfo.setStatus(MetadataBackupStatu.CANCELED);
            updateStatus();
        }

        public void markInProgress() throws IOException {
            statuInfo.setStatus(MetadataBackupStatu.IN_PROGRESS);
            updateStatus();
        }

        public void markSuccess(long size) throws IOException {
            statuInfo.setStatus(MetadataBackupStatu.SUCCEED);
            statuInfo.setSize(String.valueOf(size));
            updateStatus();
        }

        public void markFail() {
            try {
                statuInfo.setStatus(MetadataBackupStatu.FAILED);
                updateStatus();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
                log.error("mark metadata backup {} failed failed.", statusPath.toString());
            }
        }

        public String startBackup() {
            if (runningTask.containsKey(projectMetadataBackupKey)) {
                throw new KylinException(METADATA_BACKUP_IS_IN_PROGRESS);
            }
            Future<?> submit = executor.submit(() -> {
                try {
                    markInProgress();
                    doBackupMetadata();
                    long size = getSize();
                    markSuccess(size);
                } catch (Exception e) {
                    log.error("Backup metadata to {} failed", rootPath);
                    log.error(e.getMessage(), e);
                    markFail();
                } finally {
                    runningTask.remove(projectMetadataBackupKey);
                }
            });
            runningTask.put(projectMetadataBackupKey, new Pair<>(submit, this));
            return rootPath;
        }

        public FileSystem getFs() {
            return fs;
        }

        public Path getStatusPath() {
            return statusPath;
        }

        public MetadataBackupStatuInfo getStatuInfo() {
            return statuInfo;
        }

        public void setCoreMetadataTool(MetadataToolHelper coreMetadataTool) {
            this.coreMetadataTool = coreMetadataTool;
        }

        public void setJobInfoTool(JobInfoTool jobInfoTool) {
            this.jobInfoTool = jobInfoTool;
        }

        public void setQueryHistoryOffsetTool(QueryHistoryOffsetTool queryHistoryOffsetTool) {
            this.queryHistoryOffsetTool = queryHistoryOffsetTool;
        }

        public void setFavoriteRuleTool(FavoriteRuleTool favoriteRuleTool) {
            this.favoriteRuleTool = favoriteRuleTool;
        }
    }

    public static class MetadataRestoreOperator {
        public static final Logger log = LoggerFactory.getLogger(MetadataRestoreOperator.class);
        public static Future runningOperator;
        public static final ExecutorService executor = Executors.newCachedThreadPool();
        public boolean afterTruncate;
        public String project;
        public String path;
        public String uuid;

        private QueryHistoryOffsetTool queryHistoryOffsetTool;
        private FavoriteRuleTool favoriteRuleTool;
        private JobInfoTool jobInfoTool;

        public static boolean hasMetadataRestoreRunning() {
            return runningOperator != null;
        }

        public String submitMetadataRestore() {
            if (hasMetadataRestoreRunning()) {
                throw new KylinException(SYSTEM_IN_METADATA_RECOVER);
            }
            runningOperator = executor.submit(() -> doRestore());
            return uuid;
        }

        public MetadataRestoreOperator(String path, String project, boolean afterTruncate) {
            this.path = path;
            this.afterTruncate = afterTruncate;
            this.uuid = UUID.randomUUID().toString();
            this.project = project;
            init();
        }

        public void init() {
            this.queryHistoryOffsetTool = new QueryHistoryOffsetTool();
            this.favoriteRuleTool = new FavoriteRuleTool();
            this.jobInfoTool = new JobInfoTool();
        }

        public void doRestore() {
            synchronized (MetadataRestoreOperator.class) {
                AsyncTaskManager asyncTaskManager = AsyncTaskManager.getInstance(project);
                MetadataRestoreTask metadataRestoreTask = MetadataRestoreTask.newTask(uuid);
                metadataRestoreTask.setTaskAttributes(new MetadataRestoreTask.MetadataRestoreTaskAttributes());
                metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.IN_PROGRESS);
                metadataRestoreTask.setProject(project);
                asyncTaskManager.save(metadataRestoreTask);
                MaintainModeTool maintainModeTool = new MaintainModeTool("metadata restore");
                try {
                    log.info("start to restore metadata from {}.", path);
                    maintainModeTool.init();
                    maintainModeTool.markEpochs();
                    KylinConfig config = KylinConfig.getInstanceFromEnv();
                    if (_GLOBAL.equals(project)) {
                        MetadataBackupStatuInfo backupStatuInfo = OpsService.getMetadataStatusInfoWithRetry(
                                new Path(path + "/" + BACKUP_STATUS), HadoopUtil.getWorkingFileSystem(), 1);
                        backupStatuInfo.projects.forEach(relProject -> restoreProject(relProject, config, false));
                        new MetadataToolHelper().restore(config, _GLOBAL, path + "/" + CORE_META_DIR, afterTruncate,
                                true);
                    } else {
                        restoreProject(project, config, true);
                    }
                    log.info("restore metadata from {} succeed.", path);
                    metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.SUCCEED);
                } catch (Exception e) {
                    metadataRestoreTask.setStatus(MetadataRestoreTask.MetadataRestoreStatus.FAILED);
                    log.error("restore metadata {} failed.", path);
                    log.error(e.getMessage(), e);
                } finally {
                    asyncTaskManager.save(metadataRestoreTask);
                    maintainModeTool.releaseEpochs();
                    runningOperator = null;
                }
            }
        }

        public void restoreProject(String project, KylinConfig config, boolean backup) {
            log.info("start to restore metadata for {} project.", project);
            UnitOfWork.doInTransactionWithRetry(UnitOfWorkParams.builder().processor(() -> {
                new MetadataToolHelper().restore(config, project, path + "/" + CORE_META_DIR, afterTruncate, backup);
                UnitOfWork.get().doAfterUpdate(() -> {
                    queryHistoryOffsetTool.restoreProject(path, project, afterTruncate);
                    favoriteRuleTool.restoreProject(path, project, afterTruncate);
                    jobInfoTool.restoreProject(path, project, afterTruncate);
                });
                return null;
            }).useProjectLock(true).unitName(_GLOBAL).all(true).build());
            log.info("restore metadata for {} project succeed.", project);
        }

        public void setQueryHistoryOffsetTool(QueryHistoryOffsetTool queryHistoryOffsetTool) {
            this.queryHistoryOffsetTool = queryHistoryOffsetTool;
        }

        public void setFavoriteRuleTool(FavoriteRuleTool favoriteRuleTool) {
            this.favoriteRuleTool = favoriteRuleTool;
        }

        public void setJobInfoTool(JobInfoTool jobInfoTool) {
            this.jobInfoTool = jobInfoTool;
        }
    }

    public static class MetadataBackupStatuInfo {
        private String path;
        private MetadataBackupStatu status;
        private String size;
        private String owner;
        private List<String> projects;

        public String getPath() {
            return path;
        }

        public void setPath(String path) {
            this.path = path;
        }

        public MetadataBackupStatu getStatus() {
            return status;
        }

        public void setStatus(MetadataBackupStatu status) {
            this.status = status;
        }

        public String getSize() {
            return size;
        }

        public void setSize(String size) {
            this.size = size;
        }

        public String getOwner() {
            return owner;
        }

        public void setOwner(String owner) {
            this.owner = owner;
        }

        public void setProjects(List<String> projects) {
            this.projects = projects;
        }

        public List<String> getProjects() {
            return projects;
        }

    }

    public enum MetadataBackupStatu {
        IN_PROGRESS, SUCCEED, FAILED, CANCELED, UNKNOWN
    }
}
