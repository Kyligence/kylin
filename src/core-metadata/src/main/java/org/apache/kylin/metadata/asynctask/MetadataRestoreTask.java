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

package org.apache.kylin.metadata.asynctask;

import org.apache.kylin.common.util.JsonUtil;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.kyligence.kap.metadata.favorite.AsyncTaskManager;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.Delegate;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class MetadataRestoreTask extends AbstractAsyncTask {

    public static MetadataRestoreTask copyFromAbstractTask(AbstractAsyncTask task) {
        return JsonUtil.convert(task, MetadataRestoreTask.class);
    }

    public static MetadataRestoreTask newTask(String uuid) {
        MetadataRestoreTask task = new MetadataRestoreTask();
        task.setTaskKey(uuid);
        task.setTaskType(AsyncTaskManager.METADATA_RECOVER_TASK);
        return task;
    }

    @Delegate
    private MetadataRestoreTaskAttributes taskAttributesDelegate;

    @Override
    public void setTaskAttributes(TaskAttributes taskAttributes) {
        this.taskAttributes = taskAttributes;
        this.taskAttributesDelegate = (MetadataRestoreTaskAttributes) taskAttributes;
    }

    @Getter
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class MetadataRestoreTaskAttributes extends TaskAttributes {
        @JsonProperty("status")
        private MetadataRestoreStatus status;

    }

    public enum MetadataRestoreStatus {
        IN_PROGRESS, SUCCEED, FAILED
    }
}
