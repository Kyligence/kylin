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

package org.apache.kylin.rest.controller;

import static org.apache.kylin.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;

import java.util.List;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.request.FavoriteRequest;
import org.apache.kylin.rest.request.SqlAccelerateRequest;
import org.apache.kylin.rest.service.ModelSmartService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

public class NModelSmartControllerTest extends NLocalFileMetadataTestCase {

    private MockMvc mockMvc;
    @Mock
    private ModelSmartService modelSmartService;
    @InjectMocks
    private ModelRecController modelRecController = Mockito.spy(new ModelRecController());
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(modelRecController).defaultRequest(MockMvcRequestBuilders.get("/"))
                .build();
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @Before
    public void setupResource() {
        overwriteSystemProp("HADOOP_USER_NAME", "root");
        super.createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testSuggestModelWithReuseExistedModel() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        SqlAccelerateRequest favoriteRequest = new SqlAccelerateRequest("gc_test", sqls, true);
        // reuse existed model
        Mockito.doReturn(null).when(modelSmartService).suggestModel(favoriteRequest.getProject(), Mockito.spy(sqls),
                true, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).suggestModel(Mockito.any());
    }

    @Test
    public void testSuggestModelWithoutReuseExistedModel() throws Exception {
        // don't reuse existed model
        String sql = "SELECT lstg_format_name, test_cal_dt.week_beg_dt, sum(price)\n" + "FROM test_kylin_fact\n"
                + "INNER JOIN edw.test_cal_dt AS test_cal_dt ON test_kylin_fact.cal_dt = test_cal_dt.cal_dt\n"
                + "GROUP BY lstg_format_name, test_cal_dt.week_beg_dt";
        List<String> sqls = Lists.newArrayList(sql);
        SqlAccelerateRequest accerelateRequest = new SqlAccelerateRequest("gc_test", sqls, false);
        Mockito.doReturn(null).when(modelSmartService).suggestModel(accerelateRequest.getProject(), Mockito.spy(sqls),
                false, true);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/suggest_model").contentType(MediaType.APPLICATION_JSON)
                .content(JsonUtil.writeValueAsString(accerelateRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).suggestModel(Mockito.any());
    }

    @Test
    public void test_api_can_answered_by_existed_model() throws Exception {
        List<String> sqls = Lists.newArrayList("select price, count(*) from test_kylin_fact limit 1");
        FavoriteRequest favoriteRequest = new FavoriteRequest("gc_test", sqls);
        mockMvc.perform(MockMvcRequestBuilders.post("/api/models/can_answered_by_existed_model")
                .contentType(MediaType.APPLICATION_JSON).content(JsonUtil.writeValueAsString(favoriteRequest))
                .accept(MediaType.parseMediaType(HTTP_VND_APACHE_KYLIN_JSON)))
                .andExpect(MockMvcResultMatchers.status().isOk());
        Mockito.verify(modelRecController).couldAnsweredByExistedModel(Mockito.any());
    }
}
