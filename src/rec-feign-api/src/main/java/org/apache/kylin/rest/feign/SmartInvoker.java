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

package org.apache.kylin.rest.feign;

import org.apache.kylin.rest.request.OpenSqlAccelerateRequest;
import org.apache.kylin.rest.response.SuggestAndOptimizedResponse;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class SmartInvoker {

    private static SmartContract delegate;

    public static SmartInvoker getInstance() {
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new SmartInvoker();
        } else {
            return SpringContext.getBean(SmartInvoker.class);
        }
    }

    @Autowired
    public void setDelegate(SmartContract smartContract) {
        delegate = smartContract;
    }

    public SuggestAndOptimizedResponse generateSuggestion(OpenSqlAccelerateRequest request, boolean createNewModel) {
        return delegate.generateSuggestion(request, createNewModel);
    }
}
