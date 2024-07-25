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

package org.apache.kylin.metadata.query;

import java.io.Serializable;
import java.util.List;

import org.apache.kylin.metadata.acl.NDataModelAclParams;

import com.fasterxml.jackson.annotation.JsonUnwrapped;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class NativeQueryRealization implements Serializable {
    private String modelId;
    private String modelAlias;
    private Long layoutId;
    private String indexType;
    private int storageType;
    private boolean isPartialMatchModel;
    private boolean isValid = true;
    private boolean isLayoutExist = true;
    private boolean isStreamingLayout = false;
    private List<String> snapshots;
    private long lastDataRefreshTime;
    private boolean isLoadingData;
    private boolean isBuildingIndex;

    @JsonUnwrapped
    @Getter
    @Setter
    private NDataModelAclParams aclParams;

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType,
            boolean isPartialMatchModel, List<String> snapshots) {
        this(modelId, layoutId, indexType, isPartialMatchModel);
        this.modelAlias = modelAlias;
        this.snapshots = snapshots;
    }

    public NativeQueryRealization(List<String> snapshots) {
        this("null", -1L, QueryMetrics.TABLE_SNAPSHOT, false);
        this.modelAlias = "null";
        this.snapshots = snapshots;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType) {
        this(modelId, layoutId, indexType, false);
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, List<String> snapshots) {
        this(modelId, layoutId, indexType, false);
        this.snapshots = snapshots;
    }

    public NativeQueryRealization(String modelId, String modelAlias, Long layoutId, String indexType) {
        this(modelId, layoutId, indexType, false);
        this.modelAlias = modelAlias;
    }

    public NativeQueryRealization(String modelId, Long layoutId, String indexType, boolean isPartialMatchModel) {
        this.modelId = modelId;
        this.layoutId = layoutId != null && layoutId == -1L ? null : layoutId;
        this.indexType = indexType;
        this.isPartialMatchModel = isPartialMatchModel;
    }
}
