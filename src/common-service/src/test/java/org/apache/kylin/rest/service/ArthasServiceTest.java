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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;

import com.alibaba.arthas.spring.ArthasProperties;
import com.taobao.arthas.agent.attach.ArthasAgent;

class ArthasServiceTest {
    private ArthasService arthasService = new ArthasService();
    private ApplicationContext applicationContext;
    private DefaultListableBeanFactory beanFactory;
    private ArthasProperties arthasProperties;
    private ArthasAgent arthasAgent;
    private String arthasAgentBeanName = "arthasAgent";

    @BeforeEach
    void setUp() throws Exception {
        arthasProperties = mock(ArthasProperties.class);
        setField(arthasService, "arthasProperties", arthasProperties);

        arthasAgent = spy(ArthasAgent.class);
        Mockito.doNothing().when(arthasAgent).init();

        beanFactory = spy(DefaultListableBeanFactory.class);

        applicationContext = mock(ApplicationContext.class);
        Mockito.when(applicationContext.getAutowireCapableBeanFactory()).thenReturn(beanFactory);

        setField(arthasService, "applicationContext", applicationContext);
    }

    @Test
    void testArthasService() {
        assertFalse(beanFactory.containsBean(arthasAgentBeanName));

        Map<String, String> overrideConfigMap = new HashMap<>();
        arthasService.registerArthas(overrideConfigMap);
        assertTrue(beanFactory.containsBean(arthasAgentBeanName));

        arthasService.destoryArthas();
        assertFalse(beanFactory.containsBean(arthasAgentBeanName));

        overrideConfigMap.put("tunnelServer", "ws://127.0.0.1：7777/ws");
        overrideConfigMap.put("appName", "testAppName");
        arthasService.registerArthas(overrideConfigMap);
        assertTrue(beanFactory.containsBean(arthasAgentBeanName));

        arthasService.destoryArthas();
        assertFalse(beanFactory.containsBean(arthasAgentBeanName));
    }

    @Test
    void testArthasServiceWithException() {
        Mockito.doThrow(new IllegalStateException()).when(applicationContext).getAutowireCapableBeanFactory();
        arthasService.registerArthas(new HashMap<>());
        assertFalse(beanFactory.containsBean(arthasAgentBeanName));

        arthasService.destoryArthas();
        assertFalse(beanFactory.containsBean(arthasAgentBeanName));
    }

    private void setField(Object targetObject, String fieldName, Object fieldValue) throws Exception {
        Field field = targetObject.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(targetObject, fieldValue);
    }

}
