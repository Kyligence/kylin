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

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.delegate.JobStatisticsContract;
import org.springframework.stereotype.Component;

@Component
public class JobStatisticsService implements JobStatisticsContract {

    @Override
    public void updateStatistics(String project, long date, String model, long duration, long byteSize,
            int deltaCount) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            JobStatisticsManager jobStatisticsManager = JobStatisticsManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), project);
            if (model == null) {
                jobStatisticsManager.updateStatistics(date, duration, byteSize, deltaCount);
            } else {
                jobStatisticsManager.updateStatistics(date, model, duration, byteSize, deltaCount);
            }
            return null;
        }, project);
    }

    public <T> T getManager(Class<T> clz, String project) {
        return KylinConfig.getInstanceFromEnv().getManager(project, clz);
    }

}
