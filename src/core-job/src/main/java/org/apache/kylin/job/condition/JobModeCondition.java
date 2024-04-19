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

package org.apache.kylin.job.condition;

import org.apache.kylin.common.KylinConfig;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.core.type.MethodMetadata;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JobModeCondition implements Condition {
    @Override
    public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
        String beanName = "N/A";
        if (metadata instanceof AnnotationMetadata) {
            beanName = ((AnnotationMetadata) metadata).getClassName();
        } else if (metadata instanceof MethodMetadata) {
            beanName = ((MethodMetadata) metadata).getMethodName();
        }

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (kylinConfig.isUTEnv()) {
            log.info("skip load bean = {} on UT env", beanName);
            return false;
        }

        if (kylinConfig.isDataLoadingNode() || kylinConfig.isMetadataNode() || kylinConfig.isOpsNode()) {
            log.info("load bean = {} on common/data-loading/ops", beanName);
            return true;
        }

        if (null == kylinConfig.getMicroServiceMode()) {
            log.info("load bean = {} on all/job mode", beanName);
            return true;
        }

        log.info("skip load bean = {} on query mode or not data-loading mirco-service", beanName);
        return false;

    }
}
