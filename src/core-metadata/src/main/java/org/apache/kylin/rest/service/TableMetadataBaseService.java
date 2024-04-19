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
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.rest.request.MergeAndUpdateTableExtRequest;

public class TableMetadataBaseService {
    public void mergeAndUpdateTableExt(String project, MergeAndUpdateTableExtRequest request) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .mergeAndUpdateTableExt(request.getOrigin(), request.getOther());
            return null;
        }, project);
    }

    public void saveTableExt(String project, TableExtDesc tableExt) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).saveTableExt(tableExt);
            return null;
        }, project);
    }

    public void updateTableDesc(String project, TableDesc tableDesc) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateTableDesc(tableDesc);
            return null;
        }, project);
    }
}
