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

package org.apache.kylin.job.execution.handler;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.job.dao.JobStatisticsManager;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.NSparkExecutable;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;

public class ExecutableHandleUtils {

    public static void recordDownJobStats(AbstractExecutable buildTask, NDataLayout[] addOrUpdateCuboids,
            String project) {
        // make sure call this method in the last step, if 4th step is added, please modify the logic accordingly
        String model = buildTask.getTargetSubject();
        // get end time from current task instead of parent job，since parent job is in running state at this time
        long buildEndTime = buildTask.getEndTime();
        long duration = buildTask.getParent().getDuration();
        long byteSize = Arrays.stream(addOrUpdateCuboids).mapToLong(NDataLayout::getByteSize).sum();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        long startOfDay = TimeUtil.getDayStart(buildEndTime);
        JobContextUtil.withTxAndRetry(() -> {
            // update
            ExecutableManager executableManager = ExecutableManager.getInstance(kylinConfig, project);
            executableManager.updateJobOutput(buildTask.getParentId(), null, null, null, null, byteSize);

            return true;
        });
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            JobStatisticsManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateStatistics(startOfDay,
                    model, duration, byteSize, 0);
            return true;
        }, project);
    }

    public static List<AbstractExecutable> getNeedMergeTasks(DefaultExecutableOnModel parent) {
        return parent.getTasks().stream().filter(task -> task instanceof NSparkExecutable)
                .filter(task -> ((NSparkExecutable) task).needMergeMetadata()).collect(Collectors.toList());
    }
}
