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

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.alibaba.arthas.spring.ArthasProperties;
import com.alibaba.arthas.spring.StringUtils;
import com.taobao.arthas.agent.attach.ArthasAgent;

@Service
public class ArthasService {

    private static final Logger logger = LoggerFactory.getLogger(ArthasService.class);
    @Autowired(required = false)
    private Map<String, String> arthasConfigMap = new HashMap<>();
    @Autowired
    private ArthasProperties arthasProperties;
    @Autowired
    private ApplicationContext applicationContext;
    private ArthasAgent arthasAgent;
    private String arthasBeanName = "arthasAgent";

    public void registerArthas(Map<String, String> overrideConfigMap) {
        logger.info("Register arthas.");
        updateArthasProperties(overrideConfigMap);
        destoryArthas();
        try {
            DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) applicationContext
                    .getAutowireCapableBeanFactory();
            defaultListableBeanFactory.registerSingleton(arthasBeanName, arthasAgentInit());
        } catch (IllegalStateException e) {
            logger.error("Error register arthas");
        }

    }

    public void destoryArthas() {
        try {
            if (arthasAgent != null) {
                logger.info("Destory arthasAgent.");
                arthasAgent.destory();
                arthasAgent = null;
            }
            DefaultListableBeanFactory defaultListableBeanFactory = (DefaultListableBeanFactory) applicationContext
                    .getAutowireCapableBeanFactory();
            if (defaultListableBeanFactory.containsBean(arthasBeanName)) {
                logger.info("Destory {} bean from BeanFactory.", arthasBeanName);
                defaultListableBeanFactory.destroySingleton(arthasBeanName);
            }

        } catch (IllegalStateException e) {
            logger.error("Error destory arthas");
        }
    }

    private ArthasAgent arthasAgentInit() {
        Map<String, String> mapWithPrefix = null;
        if (arthasConfigMap != null) {
            arthasConfigMap = StringUtils.removeDashKey(arthasConfigMap);
            mapWithPrefix = new HashMap<>(arthasConfigMap.size());
            for (Map.Entry<String, String> entry : arthasConfigMap.entrySet()) {
                mapWithPrefix.put("arthas." + entry.getKey(), entry.getValue());
            }
            logger.info("Register arthas with tunnelServer: {} and appName: {}",
                    arthasConfigMap.getOrDefault("tunnelServer", ""), arthasConfigMap.getOrDefault("appName", ""));
        }
        arthasAgent = new ArthasAgent(mapWithPrefix, arthasProperties.getHome(), arthasProperties.isSlientInit(), null);
        arthasAgent.init();
        return arthasAgent;
    }

    private void updateArthasProperties(Map<String, String> overrideConfigMap) {
        if (overrideConfigMap.isEmpty()) {
            return;
        }
        arthasConfigMap = StringUtils.removeDashKey(arthasConfigMap);
        arthasConfigMap.putAll(overrideConfigMap);
    }
}
