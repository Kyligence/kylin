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

package org.apache.kylin.job.constant;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.execution.ExecutableState;

public class JobStatusUtil {

    public static List<ExecutableState> mapJobStatusToScheduleState(List<String> jobStatusStrList) {
        if (CollectionUtils.isEmpty(jobStatusStrList)) {
            return Lists.newArrayList();
        }
        List<ExecutableState> stateList = Lists.newArrayList();
        jobStatusStrList.forEach(str -> stateList.addAll(mapJobStatusToScheduleState(JobStatusEnum.valueOf(str))));
        return stateList;
    }

    public static List<ExecutableState> mapJobStatusToScheduleState(JobStatusEnum jobStatus) {
        List<ExecutableState> scheduleStateList = Lists.newArrayList();
        switch (jobStatus) {
            case SKIP:
                scheduleStateList.add(ExecutableState.SKIP);
                break;
            case NEW:
                scheduleStateList.add(ExecutableState.READY);
                break;
            case PENDING:
                scheduleStateList.add(ExecutableState.READY);
                scheduleStateList.add(ExecutableState.PENDING);
                break;
            case RUNNING:
                scheduleStateList.add(ExecutableState.RUNNING);
                break;
            case ERROR:
                scheduleStateList.add(ExecutableState.ERROR);
                break;
            case FINISHED:
                scheduleStateList.add(ExecutableState.SUCCEED);
                break;
            case STOPPED:
                scheduleStateList.add(ExecutableState.PAUSED);
                break;
            case SUICIDAL:
                scheduleStateList.add(ExecutableState.SUICIDAL);
                break;
            case DISCARDED:
                scheduleStateList.add(ExecutableState.SUICIDAL);
                scheduleStateList.add(ExecutableState.DISCARDED);
                break;
            default:
                throw new RuntimeException("Can not map " + jobStatus.name() + " to ExecutableState");
        }
        return scheduleStateList;
    }

}
