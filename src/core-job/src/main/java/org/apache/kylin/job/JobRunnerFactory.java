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
package org.apache.kylin.job;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigBase;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.common.util.ExecutableApplication;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

import com.google.common.collect.Maps;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import lombok.RequiredArgsConstructor;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobRunnerFactory {

    private JobRunnerFactory() {
        // Just implement it
    }

    public static AbstractJobRunner createRunner(KylinConfig config, String type, String project,
            List<String> resources) {
        switch (type) {
        case "fork":
            return new ForkBasedJobRunner(config, project, resources);
        case "in-memory":
            return new InMemoryJobRunner(config, project, resources);
        default:
            throw new NotImplementedException("Runner type " + type + " not implement");
        }
    }

    @RequiredArgsConstructor
    public abstract static class AbstractJobRunner {

        protected final KylinConfig kylinConfig;
        protected final String project;
        protected final List<String> originResources;

        protected String metaDumpUrl;
        protected String jobId;

        public void init(String jobId) {
            this.jobId = jobId;
            val jobTmpDir = getJobTmpDir();
            metaDumpUrl = kylinConfig.getMetadataUrlPrefix() + "@hdfs,path=file://" + jobTmpDir + "/meta";
        }

        public void start(ExecutableApplication app, Map<String, String> args) throws IOException, ShellException {
            attachMetadataAndKylinProps();
            args.put("meta", metaDumpUrl);
            args.put("metaOutput", metaDumpUrl + "_output");
            doExecute(app, args);
        }

        public void cleanupEnv() {
            FileUtils.deleteQuietly(new File(getJobTmpDir()));
        }

        protected abstract void doExecute(ExecutableApplication app, Map<String, String> args) throws ShellException;

        protected String formatArgs(Map<String, String> args) {
            return args.entrySet().stream().map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                    .collect(Collectors.joining(" "));
        }

        public String getJobTmpDir() {
            return KylinConfigBase.getKylinHome() + "/tmp/" + jobId;
        }

        protected void attachMetadataAndKylinProps() throws IOException {
            Preconditions.checkArgument(StringUtils.isNotEmpty(metaDumpUrl), "Missing metaUrl");
            File tmpDir = File.createTempFile("kylin_job_meta", "");
            try {
                // we need a directory, so delete the file first
                FileUtils.forceDelete(tmpDir);
                Properties props = kylinConfig.exportToProperties();
                props.setProperty("kylin.query.queryhistory.url", kylinConfig.getMetadataUrl().toString());
                props.setProperty("kylin.metadata.url", metaDumpUrl);

                // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
                Map<String, RawResource> dumpMap = EnhancedUnitOfWork
                        .doInTransactionWithCheckAndRetry(UnitOfWorkParams.<Map<String, RawResource>> builder()
                                .readonly(true).unitName(project).maxRetry(1).processor(() -> {
                                    ResourceStore resourceStore = ResourceStore.getKylinMetaStore(kylinConfig);
                                    Map<String, RawResource> retMap = Maps.newHashMap();
                                    originResources.forEach(resPath -> {
                                        RawResource rawResource = resourceStore.getResource(resPath);
                                        retMap.put(resPath, rawResource);
                                    });
                                    return retMap;
                                }).build());
                if (MapUtils.isEmpty(dumpMap)) {
                    return;
                }
                // dump metadata
                ResourceStore.dumpResourceMaps(tmpDir, dumpMap, props);

                // copy metadata to target metaUrl
                KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
                MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
                // clean up
                log.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
            } finally {
                FileUtils.forceDelete(tmpDir);
            }
        }
    }
}
