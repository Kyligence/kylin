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

import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_ERROR;
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_EXECUTE_ERROR;
import static org.apache.kylin.common.msg.Message.LOAD_GLUTEN_CACHE_ROUTE_RESPONSE_EMPTY;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

import java.util.Locale;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.response.RestResponse;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.tool.restclient.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

class LoadCacheStepTest {

    @Test
    void routeCacheToAllQueryNode() throws Exception {
        val mockStep = new LoadCacheStep() {
            @Override
            protected ExecuteResult doWork(JobContext context) throws ExecuteException {
                return null;
            }
        };
        val project = "default";
        val commands = Sets.newHashSet("test command");
        val httpResponse = Mockito.mock(HttpResponse.class);
        val httpEntity = Mockito.mock(HttpEntity.class);
        val kylinConfig = Mockito.mock(KylinConfig.class);
        var restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(true));

        try (MockedStatic<KylinConfig> kylinConfigMockedStatic = Mockito.mockStatic(KylinConfig.class);
                MockedStatic<EntityUtils> entityUtilsMockedStatic = Mockito.mockStatic(EntityUtils.class);
                MockedConstruction<RestClient> ignored2 = Mockito.mockConstruction(RestClient.class,
                        (mock, context) -> Mockito.doReturn(httpResponse).when(mock).forwardPost(any(), anyString()))) {
            kylinConfigMockedStatic.when(KylinConfig::getInstanceFromEnv).thenReturn(kylinConfig);

            Mockito.when(kylinConfig.isUTEnv()).thenReturn(false);
            Mockito.when(kylinConfig.getServerPort()).thenReturn("6161");
            Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

            entityUtilsMockedStatic.when(() -> EntityUtils.toByteArray(httpEntity)).thenReturn(restResult);
            mockStep.routeCacheToAllQueryNode(project, commands);

            restResult = JsonUtil.writeValueAsBytes(RestResponse.ok(false));
            entityUtilsMockedStatic.when(() -> EntityUtils.toByteArray(httpEntity)).thenReturn(restResult);
            try {
                mockStep.routeCacheToAllQueryNode(project, commands);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(LOAD_GLUTEN_CACHE_ROUTE_EXECUTE_ERROR, e.getMessage());
            }

            restResult = JsonUtil.writeValueAsBytes(RestResponse.fail());
            entityUtilsMockedStatic.when(() -> EntityUtils.toByteArray(httpEntity)).thenReturn(restResult);
            try {
                mockStep.routeCacheToAllQueryNode(project, commands);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(String.format(Locale.ROOT, LOAD_GLUTEN_CACHE_ROUTE_ERROR, ""), e.getMessage());
            }

            entityUtilsMockedStatic.when(() -> EntityUtils.toByteArray(httpEntity)).thenReturn(null);
            try {
                mockStep.routeCacheToAllQueryNode(project, commands);
                Assertions.fail();
            } catch (Exception e) {
                Assertions.assertInstanceOf(KylinRuntimeException.class, e);
                Assertions.assertEquals(LOAD_GLUTEN_CACHE_ROUTE_RESPONSE_EMPTY, e.getMessage());
            }
        }
    }
}
