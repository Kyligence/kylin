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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import io.kyligence.kap.metadata.favorite.AsyncAccelerationTask;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@EqualsAndHashCode
public class AbstractAsyncTask {

    private int id;

    @JsonProperty("task_type")
    private String taskType;

    @JsonProperty("task_key")
    private String taskKey;

    private String project;

    @JsonProperty("task_attributes")
    protected TaskAttributes taskAttributes;

    private long updateTime;

    private long createTime;

    private long mvcc;

    public AbstractAsyncTask(String taskType) {
        this.taskType = taskType;
    }

    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
    @JsonSubTypes({ @JsonSubTypes.Type(AsyncAccelerationTask.AccelerationTaskAttributes.class),
            @JsonSubTypes.Type(MetadataRestoreTask.MetadataRestoreTaskAttributes.class) })
    @NoArgsConstructor
    public static class TaskAttributes {
    }
}
