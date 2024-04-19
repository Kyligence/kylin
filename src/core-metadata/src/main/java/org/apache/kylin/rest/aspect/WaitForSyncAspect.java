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
package org.apache.kylin.rest.aspect;

import org.apache.kylin.metadata.cube.utils.StreamingUtils;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Aspect
@Component
public class WaitForSyncAspect {

    @Pointcut("@annotation(waitForSyncBeforeRPC)")
    public void callBefore(WaitForSyncBeforeRPC waitForSyncBeforeRPC) {
        /// just implement it
    }

    @Before("callBefore(waitForSyncBeforeRPC)")
    public void before(WaitForSyncBeforeRPC waitForSyncBeforeRPC) {
        StreamingUtils.replayAuditlog();
    }

    @Pointcut("@annotation(waitForSyncAfterRPC)")
    public void callAfter(WaitForSyncAfterRPC waitForSyncAfterRPC) {
        /// just implement it
    }

    @After("callAfter(waitForSyncAfterRPC)")
    public void after(WaitForSyncAfterRPC waitForSyncAfterRPC) {
        StreamingUtils.replayAuditlog();
    }
}
