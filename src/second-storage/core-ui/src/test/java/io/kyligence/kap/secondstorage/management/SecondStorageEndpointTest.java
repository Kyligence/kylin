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

package io.kyligence.kap.secondstorage.management;

import static org.apache.kylin.common.exception.ServerErrorCode.LOW_LEVEL_LICENSE;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.extension.KylinInfoExtension;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.ModelService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.springframework.http.MediaType;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import io.kyligence.kap.secondstorage.management.request.ModelEnableRequest;
import io.kyligence.kap.secondstorage.management.request.ProjectEnableRequest;
import io.kyligence.kap.secondstorage.management.request.StorageRequest;
import lombok.val;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.common.asyncprofiler.AsyncProfiler" })
@PrepareForTest({ KylinInfoExtension.class })
public class SecondStorageEndpointTest extends NLocalFileMetadataTestCase {

    @InjectMocks
    private final SecondStorageEndpoint secondStorageEndpoint = Mockito.spy(new SecondStorageEndpoint());
    private final Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
    private MockMvc mockMvc;
    @Mock
    private ModelService modelService;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(secondStorageEndpoint)
                .defaultRequest(MockMvcRequestBuilders.get("/api/storage/segments"))
                .build();

        SecurityContextHolder.getContext().setAuthentication(authentication);
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void loadStorage() throws Exception {
        StorageRequest storageRequest = new StorageRequest();
        storageRequest.setModel("test");
        storageRequest.setProject("default");
        storageRequest.setSegmentIds(Lists.asList("seg1", new String[]{"seg2"}));
        val param = JsonUtil.writeValueAsString(storageRequest);

        Mockito.when(modelService.convertSegmentIdWithName("test", "default", new String[]{"seg1", "seg2"}, new String[]{}))
                .thenReturn(new String[]{"seg1", "seg2"});

        mockMvc.perform(MockMvcRequestBuilders.post("/api/storage/segments").content(param)
                .contentType(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk()).andReturn();
        Mockito.verify(secondStorageEndpoint).loadStorage(Mockito.any(StorageRequest.class));
    }

    @Test
    public void testEnableModelStorageFalseKylinInfoException() throws Exception {
        val enableRequest = new ModelEnableRequest();
        enableRequest.setProject("default");
        enableRequest.setModel("test");
        enableRequest.setEnabled(true);
        val factory = Mockito.mock(KylinInfoExtension.Factory.class);
        Mockito.when(factory.checkKylinInfo()).thenReturn(false);
        PowerMockito.stub(PowerMockito.method(KylinInfoExtension.class, "getFactory")).toReturn(factory);
        try {
            secondStorageEndpoint.enableStorage(enableRequest);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(LOW_LEVEL_LICENSE.toErrorCode(), e.getErrorCode());
        }
    }

    @Test
    public void testEnableProjectStorageFalseKylinInfoException() throws Exception {
        val request = new ProjectEnableRequest();
        request.setProject("default");
        request.setEnabled(true);
        val factory = Mockito.mock(KylinInfoExtension.Factory.class);
        Mockito.when(factory.checkKylinInfo()).thenReturn(false);
        PowerMockito.stub(PowerMockito.method(KylinInfoExtension.class, "getFactory")).toReturn(factory);
        try {
            secondStorageEndpoint.enableProjectStorage(request);
            Assert.fail();
        } catch (KylinException e) {
            Assert.assertEquals(LOW_LEVEL_LICENSE.toErrorCode(), e.getErrorCode());
        }
    }
}
