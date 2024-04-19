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

import static org.apache.kylin.common.constant.Constants.BACKSLASH;
import static org.apache.kylin.common.constant.Constants.METADATA_FILE;
import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;
import static org.apache.kylin.common.exception.KylinException.CODE_SUCCESS;
import static org.apache.kylin.common.exception.KylinException.CODE_UNDEFINED;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.metrics.MetricsCategory;
import org.apache.kylin.common.metrics.MetricsGroup;
import org.apache.kylin.common.metrics.MetricsName;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.ClusterConstant;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.helper.RoutineToolHelper;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.resourcegroup.KylinInstance;
import org.apache.kylin.metadata.resourcegroup.RequestTypeEnum;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupManager;
import org.apache.kylin.metadata.resourcegroup.ResourceGroupMappingInfo;
import org.apache.kylin.rest.cluster.ClusterManager;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ServerInfoResponse;
import org.apache.kylin.tool.garbage.LogCleaner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.core.type.TypeReference;

import io.kyligence.kap.metadata.epoch.EpochManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ScheduleService extends BasicService {

    private static final String GLOBAL = "global";

    private static final String CLEAN_SPARDER_EVENT_LOG = "http://%s/kylin/api/system/clean_sparder_event_log";

    @Autowired
    @Qualifier("normalRestTemplate")
    RestTemplate restTemplate;

    @Autowired
    FileService fileService;

    @Autowired
    MetadataBackupService backupService;

    @Autowired
    ProjectService projectService;

    @Autowired(required = false)
    ProjectSmartSupporter projectSmartSupporter;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    private final String DO_CLEANUP_GARBAGE_PATH = "/kylin/api/system/do_cleanup_garbage";
    @Autowired
    private ClusterManager clusterManager;

    private final ExecutorService executors = Executors
            .newSingleThreadExecutor(new NamedThreadFactory("RoutineTaskScheduler"));
    private final ExecutorService asyncExecutors = new ThreadPoolExecutor(20, 20, 30, TimeUnit.MINUTES,
            new LinkedBlockingQueue<>(), new NamedThreadFactory("RoutineBroadcastScheduler"));

    private long opsCronTimeout;
    private String tmpMetadataBackupFilePath;

    private static final ThreadLocal<Future<?>> CURRENT_FUTURE = new ThreadLocal<>();

    private static final Map<Future<?>, Long> ASYNC_FUTURES = Maps.newConcurrentMap();


    @Scheduled(cron = "${kylin.metadata.ops-cron:0 0 0 * * *}")
    public void routineTask() throws Exception {
        executorService.submit(() -> {
            try {
                doRoutineTask();
            } catch (Exception e) {
                log.error("Execute cleanup garbage failed", e);
            }
        });
        log.info("Successfully trigger garbage cleanup");
    }

    public void doRoutineTask() throws Exception {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        opsCronTimeout = kylinConfig.getRoutineOpsTaskTimeOut();
        CURRENT_FUTURE.remove();
        ASYNC_FUTURES.clear();
        EpochManager epochManager = EpochManager.getInstance();
        try {
            log.info("Start to work");
            long startTime = System.currentTimeMillis();
            MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON, MetricsCategory.GLOBAL, GLOBAL);
            try (SetThreadName ignored = new SetThreadName("RoutineOpsWorker")) {
                if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                    AtomicReference<Pair<String, String>> backupFolder = new AtomicReference<>(null);
                    broadcastCleanSparderEventLogToQueryNodes();
                    executeTask(() -> backupFolder.set(backupService.backupAll()), "MetadataBackup", startTime);
                    executeMetadataBackupInTenantMode(kylinConfig, startTime, backupFolder);
                    executeTask(() -> RoutineToolHelper.cleanQueryHistoriesAsync(getRemainingTime(startTime),
                            TimeUnit.MILLISECONDS), "QueryHistoriesCleanup", startTime);
                    executeTask(RoutineToolHelper::cleanStreamingStats, "StreamingStatsCleanup", startTime);
                    executeTask(RoutineToolHelper::deleteRawRecItems, "RawRecItemsDeletion", startTime);
                    executeTask(RoutineToolHelper::cleanGlobalSourceUsage, "SourceUsageCleanup", startTime);
                    executeTask(() -> projectService.cleanupAcl(), "AclCleanup", startTime);
                }
                executeTask(() -> projectService.garbageCleanup(getRemainingTime(startTime)), "ProjectGarbageCleanup",
                        startTime);
                // clean storage
                if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
                    executeTask(RoutineToolHelper::cleanStorageForRoutine, "HdfsCleanup", startTime);
                }
                // clear logs for stopped instance
                executeTask(() -> new LogCleaner().cleanUp(), "RemoteLogCleanup", startTime);
                executeTask(() -> RoutineToolHelper.cleanEventLog(true, false, false), "EventLogCleanup", startTime);
                log.info("Finish to work, cost {}ms", System.currentTimeMillis() - startTime);
            }
        } catch (InterruptedException e) {
            log.warn("Routine task execution interrupted", e);
            Thread.currentThread().interrupt();
        } catch (TimeoutException e) {
            log.warn("Routine task execution timeout", e);
            if (CURRENT_FUTURE.get() != null) {
                CURRENT_FUTURE.get().cancel(true);
            }
            ASYNC_FUTURES.keySet().forEach(asyncTask -> asyncTask.cancel(true));
        } finally {
            ASYNC_FUTURES.clear();
        }
        MetricsGroup.hostTagCounterInc(MetricsName.METADATA_OPS_CRON_SUCCESS, MetricsCategory.GLOBAL, GLOBAL);
    }

    public void executeMetadataBackupInTenantMode(KylinConfig kylinConfig, long startTime,
            AtomicReference<Pair<String, String>> backupFolder) throws InterruptedException, TimeoutException {
        val rgManager = ResourceGroupManager.getInstance(kylinConfig);
        if (kylinConfig.isKylinMultiTenantEnabled() && rgManager.isResourceGroupEnabled()) {
            val servers = getResourceGroupServerNode(rgManager);
            log.info("ResourceGroupServerNode : {}", servers);
            if (servers.size() > 0) {
                try {
                    tmpMetadataBackupFilePath = "";
                    executeBroadcastMetadataBackup(() -> broadcastToServer(servers, backupFolder, startTime),
                            "broadcastMetadataBackup", startTime);
                } finally {
                    if (StringUtils.isNotBlank(tmpMetadataBackupFilePath)) {
                        fileService.deleteTmpDir(tmpMetadataBackupFilePath);
                    }
                }
                log.info("backup file path [{}] broadcast to server success", backupFolder.get().getFirst());
            }
        }
    }

    /**
     * get resource group server node without global server's resource group
     */
    public Map<String, List<KylinInstance>> getResourceGroupServerNode(ResourceGroupManager rgManager) {
        val servers = Maps.<String, List<KylinInstance>> newHashMap();
        val allResourceGroups = rgManager.getResourceGroup();
        val concurrentServer = AddressUtil.getLocalInstance();
        String concurrentServerResourceGroupId = allResourceGroups.getKylinInstances().stream()
                .filter(instance -> instance.getInstance().equals(concurrentServer))
                .map(KylinInstance::getResourceGroupId).findFirst().orElse(null);
        val buildResourceGroups = allResourceGroups.getResourceGroupMappingInfoList().stream()
                .filter(resourceGroupMappingInfo -> resourceGroupMappingInfo.getRequestType()
                        .equals(RequestTypeEnum.BUILD))
                .map(ResourceGroupMappingInfo::getResourceGroupId)
                .filter(groupId -> !StringUtils.equals(groupId, concurrentServerResourceGroupId))
                .collect(Collectors.toList());
        allResourceGroups.getKylinInstances().stream()
                .filter(kylinInstance -> buildResourceGroups.contains(kylinInstance.getResourceGroupId()))
                .forEach(instance -> {
                    val instances = servers.getOrDefault(instance.getResourceGroupId(), Lists.newArrayList());
                    instances.add(instance);
                    servers.put(instance.getResourceGroupId(), instances);
                });
        return servers;
    }

    public void broadcastToServer(Map<String, List<KylinInstance>> servers,
            AtomicReference<Pair<String, String>> backupFolder, long startTime) {
        val backupFilePath = backupFolder.get().getFirst() + BACKSLASH + METADATA_FILE;
        val backupDir = backupFolder.get().getSecond();
        try {
            val tmpFileMessage = fileService.saveMetadataBackupInTmpPath(backupFilePath);
            tmpMetadataBackupFilePath = tmpFileMessage.getFirst();
            val tmpFileLength = tmpFileMessage.getSecond();
            for (Map.Entry<String, List<KylinInstance>> entry : servers.entrySet()) {
                val kylinInstances = entry.getValue();
                if (CollectionUtils.isNotEmpty(kylinInstances)) {
                    val server = kylinInstances.get(RandomUtil.nextInt(kylinInstances.size()));
                    log.info("routineTask[broadcastMetadataBackup] execute to groupId [{}] server [{}]", entry.getKey(),
                            server.getInstance());
                    executeAsyncTask(
                            () -> broadcastToTenantNode(entry.getKey(), backupDir, tmpMetadataBackupFilePath,
                                    tmpFileLength, server.getInstance()),
                            "broadcastToTenantNode-GroupIs[" + entry.getKey() + "]", startTime);
                }
            }
        } catch (IOException e) {
            log.error("backup file path [{}] broadcast to server has error. reason:", backupFilePath, e);
        }
    }

    private void broadcastCleanSparderEventLogToQueryNodes() {
        List<ServerInfoResponse> queryNodes = clusterManager.getQueryServers();

        try {
            for (ServerInfoResponse node : queryNodes) {
                if (ClusterConstant.ALL.equals(node.getMode())) {
                    continue;
                }

                val url = String.format(Locale.ROOT, CLEAN_SPARDER_EVENT_LOG, node.getHost());
                log.info("Start broadcasting to clean the sparder event log of {}", url);

                val httpHeaders = new HttpHeaders();
                httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
                val response = restTemplate.exchange(url, HttpMethod.DELETE, new HttpEntity<>(httpHeaders),
                        String.class);
                receive(response, "noticeToQueryNode");
            }
        } catch (Exception e) {
            log.error("Broadcast cleaning sparder event log failed!", e);
        }
    }

    private void receive(ResponseEntity<String> response, String msg) throws IOException {
        val responseStatus = response.getStatusCodeValue();
        if (responseStatus != HttpStatus.SC_OK) {
            log.error("{} failed, HttpStatus is {}", msg, responseStatus);
        }

        val responseBody = Optional.ofNullable(response.getBody()).orElse("");
        val responseJson = JsonUtil.readValue(responseBody, new TypeReference<RestResponse<Boolean>>() {
        });
        if (!StringUtils.equals(responseJson.getCode(), KylinException.CODE_SUCCESS)) {
            log.error("{} failed, response code is {}", msg, responseJson.getCode());
        }
    }

    public void broadcastToTenantNode(String resourceGroupId, String backupDir, String tmpFilePath, long tmpFileLength,
            String host) {
        try {
            val url = String.format(Locale.ROOT, "http://%s/kylin/api/system/broadcast_metadata_backup", host);
            val req = Maps.newHashMap();
            req.put("resource_group_id", resourceGroupId);
            req.put("tmp_file_path", tmpFilePath);
            req.put("tmp_file_size", tmpFileLength);
            req.put("backup_dir", backupDir);
            val httpHeaders = new HttpHeaders();
            httpHeaders.add(HttpHeaders.CONTENT_TYPE, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON);
            val exchange = restTemplate.exchange(url, HttpMethod.POST,
                    new HttpEntity<>(JsonUtil.writeValueAsBytes(req), httpHeaders), String.class);
            receive(exchange, "noticeToTenantNode");
        } catch (IOException e) {
            log.error(e.getMessage(), e);
        }
    }

    public void executeTask(Runnable task, String taskName, long startTime)
            throws InterruptedException, TimeoutException {
        val future = executors.submit(task);
        val remainingTime = getRemainingTime(startTime);
        log.info("execute task {} with remaining time: {} ms", taskName, remainingTime);
        CURRENT_FUTURE.set(future);
        try {
            future.get(remainingTime, TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            log.warn("Routine task {} execution failed, reason:", taskName, e);
        }
    }

    public void executeBroadcastMetadataBackup(Runnable task, String taskName, long startTime)
            throws InterruptedException, TimeoutException {
        executeTask(task, taskName, startTime);
        cancelTimeoutAsyncTask(startTime);
    }

    public void cancelTimeoutAsyncTask(long startTime) throws InterruptedException {
        while (ASYNC_FUTURES.size() > 0) {
            ASYNC_FUTURES.forEach((asyncTask, start) -> {
                if (getRemainingTime(start) <= 0) {
                    asyncTask.cancel(true);
                }
            });
            val doneTaskCount = ASYNC_FUTURES.keySet().stream().filter(Future::isDone).count();
            if (doneTaskCount == ASYNC_FUTURES.size()) {
                log.info("all running asyncTask[broadcastToServer] is done");
                break;
            }
            if (getRemainingTime(startTime) <= 0) {
                log.warn("cancel all running asyncTask, DoneAsyncTask count: [{}], AllAsyncTask count : [{}]",
                        doneTaskCount, ASYNC_FUTURES.size());
                ASYNC_FUTURES.keySet().stream().filter(asyncTask -> !asyncTask.isDone())
                        .forEach(asyncTask -> asyncTask.cancel(true));
                break;
            }
            TimeUnit.SECONDS.sleep(10);
        }
    }

    public void executeAsyncTask(Runnable task, String taskName, long startTime) {
        val future = asyncExecutors.submit(task);
        val remainingTime = getRemainingTime(startTime);
        log.info("execute async task {} with remaining time: {} ms", taskName, remainingTime);
        ASYNC_FUTURES.put(future, System.currentTimeMillis());
    }

    private long getRemainingTime(long startTime) {
        return opsCronTimeout - (System.currentTimeMillis() - startTime);
    }

    public Pair<String, String> triggerAllCleanupGarbage(HttpServletRequest request) {
        Map<String, List<String>> epochOwnerMap = new HashMap<>();

        EpochManager epochManager = EpochManager.getInstance();
        String globalOwner = epochManager.getGlobalEpoch().getCurrentEpochOwner();

        epochOwnerMap.put(StringUtils.split(globalOwner, '|')[0],
                Lists.newArrayList(epochManager.getGlobalEpoch().getEpochTarget()));

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        List<String> projectNames = projectManager.listAllProjects().stream()
                .map(projectInstance -> projectInstance.getName()).collect(Collectors.toList());
        projectNames.forEach(projectName -> {
            String projectOwner = epochManager.getEpoch(projectName).getCurrentEpochOwner();
            String host = StringUtils.split(projectOwner, '|')[0];
            epochOwnerMap.putIfAbsent(host, Lists.newArrayList());
            epochOwnerMap.get(host).add(projectName);
        });

        StringBuilder msg = new StringBuilder();

        Pair<String, String> result = new Pair<>();
        result.setFirst(CODE_SUCCESS);
        epochOwnerMap.entrySet().forEach(entry -> {
            String host = entry.getKey();
            String target = StringUtils.join(entry.getValue(), ",");
            String url = "http://" + host + DO_CLEANUP_GARBAGE_PATH;
            try {
                EnvelopeResponse response = generateTaskForRemoteHost(request, url);
                if (response.getCode().equals(CODE_SUCCESS)) {
                    msg.append(target).append(":").append(host).append(":").append("triggered successfully")
                            .append(";");
                }
                if (response.getCode().equals(CODE_UNDEFINED)) {
                    result.setFirst(CODE_UNDEFINED);
                    msg.append(target).append(":").append(host).append(":").append("triggered failed")
                            .append(response.getMsg()).append(";");
                }
            } catch (Exception e) {
                msg.append(target).append(":").append(host).append(":").append("triggered failed: ")
                        .append(e.getMessage()).append(";");
            }
        });
        result.setSecond(msg.toString());
        return result;
    }

}
