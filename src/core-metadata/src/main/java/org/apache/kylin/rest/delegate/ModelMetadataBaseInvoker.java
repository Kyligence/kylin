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
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;
import org.apache.kylin.rest.service.ModelMetadataBaseService;
import org.apache.kylin.rest.util.SpringContext;

public class ModelMetadataBaseInvoker {

    public static ModelMetadataBaseInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new ModelMetadataBaseInvoker();
        } else {
            return SpringContext.getBean(ModelMetadataBaseInvoker.class);
        }
    }

    private final ModelMetadataBaseService modelMetadataBaseService = new ModelMetadataBaseService();

    public String getModelNameById(String modelId, String project) {
        return modelMetadataBaseService.getModelNameById(modelId, project);
    }

    public void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest) {
        modelMetadataBaseService.updateDataflow(dataFlowUpdateRequest);
    }

    public void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status) {
        modelMetadataBaseService.updateDataflowStatus(project, uuid, status);
    }
}
