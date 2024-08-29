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

package io.kyligence.kap.engine.spark.job;


import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.ExecutableConstants;
import org.apache.kylin.job.execution.NSparkExecutable;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.cube.model.NDataflowManager;

public class NSparkLayoutDataOptimizeStep extends NSparkExecutable {

    public NSparkLayoutDataOptimizeStep(Object notSetId) {
        super(notSetId);
    }

    public NSparkLayoutDataOptimizeStep(String sparkSubmitClassName) {
        this.setSparkSubmitClassName(sparkSubmitClassName);
        this.setName(ExecutableConstants.STEP_NAME_OPTIMIZE_LAYOUT_DATA);
    }

    @Override
    protected Set<String> getMetadataDumpList(KylinConfig config) {
        NDataflow df = NDataflowManager.getInstance(config, getProject()).getDataflow(getDataflowId());
        return new LinkedHashSet<>(df.collectPrecalculationResource());
    }
}