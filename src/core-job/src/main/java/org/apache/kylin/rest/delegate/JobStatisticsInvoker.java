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
package org.apache.kylin.rest.delegate;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.HDFSMetadataStore;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.rest.service.JobStatisticsService;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class JobStatisticsInvoker {

    private static JobStatisticsContract delegate = null;

    public static void setDelegate(JobStatisticsContract delegate) {
        if (JobStatisticsInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, JobStatisticsInvoker.delegate);
        }
        JobStatisticsInvoker.delegate = delegate;
    }

    private JobStatisticsContract getDelegate() {
        if (JobStatisticsInvoker.delegate == null) {
            // Generally delegate will be set in ContractConfig, here is used for test
            if (SpringContext.getApplicationContext() != null) {
                return SpringContext.getBean(JobStatisticsContract.class);
            } else {
                return new JobStatisticsService();
            }
        }
        return JobStatisticsInvoker.delegate;
    }

    public static JobStatisticsInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null || null == getJobStatisticsContract()) {
            // for UT
            return new JobStatisticsInvoker();
        } else {
            return SpringContext.getBean(JobStatisticsInvoker.class);
        }
    }

    private static JobStatisticsContract getJobStatisticsContract() {
        try {
            return SpringContext.getBean(JobStatisticsContract.class);
        } catch (Exception e) {
            return null;
        }
    }

    public void updateStatistics(String project, long date, String model, long duration, long byteSize,
            int deltaCount) {
        getDelegate().updateStatistics(project, date, model, duration, byteSize, deltaCount);
    }

}
