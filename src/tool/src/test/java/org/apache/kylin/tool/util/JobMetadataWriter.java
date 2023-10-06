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

package org.apache.kylin.tool.util;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.guava30.shaded.common.io.ByteSource;
import org.apache.kylin.job.dao.ExecutablePO;
import org.apache.kylin.job.dao.JobInfoDao;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.job.util.JobInfoUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.delegate.ModelMetadataBaseInvoker;
import org.mockito.Mockito;

public class JobMetadataWriter {

    public static void writeJobMetaData(KylinConfig config, List<RawResource> metadata) {
        JobContextUtil.cleanUp();
        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(config);
        ModelMetadataBaseInvoker modelMetadataBaseInvoker = Mockito.mock(ModelMetadataBaseInvoker.class);
        Mockito.when(modelMetadataBaseInvoker.getModelNameById(Mockito.anyString(), Mockito.anyString()))
                .thenReturn("test");

        metadata.forEach(rawResource -> {
            String path = rawResource.getResPath();
            if (!path.equals("/calories/execute/9462fee8-e6cd-4d18-a5fc-b598a3c5edb5")) {
                return;
            }
            long updateTime = rawResource.getTimestamp();
            ExecutablePO executablePO = parseExecutablePO(rawResource.getByteSource(), updateTime, "calories");
            jobInfoDao.addJob(executablePO);
        });
    }

    public static void writeJobMetaData(KylinConfig config) {
        JobContextUtil.cleanUp();
        JobInfoDao jobInfoDao = JobContextUtil.getJobInfoDao(config);
        ModelMetadataBaseInvoker modelMetadataBaseInvoker = Mockito.mock(ModelMetadataBaseInvoker.class);
        Mockito.when(modelMetadataBaseInvoker.getModelNameById(Mockito.anyString(), Mockito.anyString()))
                .thenReturn("test");

        ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
        List<String> allMetadataKey = resourceStore.collectResourceRecursively("/", "");
        NProjectManager projectManager = NProjectManager.getInstance(config);
        List<String> projectNames = projectManager.listAllProjects().stream().map(ProjectInstance::getName)
                .collect(Collectors.toList());
        for (String projectName : projectNames) {
            String prefix = "/" + projectName + "/execute/";
            List<String> jobKeyList = allMetadataKey.stream().filter(key -> key.startsWith(prefix))
                    .collect(Collectors.toList());
            for (String jobKey : jobKeyList) {
                RawResource jobRawResource = resourceStore.getResource(jobKey);
                long updateTime = jobRawResource.getTimestamp();
                ExecutablePO executablePO = parseExecutablePO(jobRawResource.getByteSource(), updateTime, projectName);
                jobInfoDao.addJob(executablePO);
            }
        }
    }

    private static ExecutablePO parseExecutablePO(ByteSource byteSource, long updateTime, String projectName) {
        try {
            return JobInfoUtil.deserializeExecutablePO(byteSource, updateTime, projectName);
        } catch (IOException e) {
            throw new RuntimeException("Error when deserializing job metadata", e);
        }
    }
}
