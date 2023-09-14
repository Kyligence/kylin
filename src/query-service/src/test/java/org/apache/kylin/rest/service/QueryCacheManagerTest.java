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

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.cache.KylinCache;
import org.apache.kylin.rest.cache.RedisCache;
import org.apache.kylin.rest.cache.RedisCacheV2;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;

@MetadataInfo
@ExtendWith(MockitoExtension.class)
class QueryCacheManagerTest {
    @Mock
    private KylinCache kylinCache;
    @InjectMocks
    private QueryCacheManager queryCacheManager;

    @Test
    void testInitIsRedisEnabled() {
        System.setProperty("kylin.cache.redis.enabled", "true");
        try {
            queryCacheManager.init();
        } catch (Exception e) {
            Assertions.fail();
        }
        System.clearProperty("kylin.cache.redis.enabled");
    }

    @Test
    void testInitIsNotRedisEnabled() {
        System.setProperty("kylin.cache.redis.enabled", "false");
        try {
            queryCacheManager.init();
        } catch (Exception e) {
            Assertions.fail();
        }
        System.clearProperty("kylin.cache.redis.enabled");
    }

    @Test
    void testInitIsRedisSentinelEnabled() {
        System.setProperty("kylin.cache.redis.enabled", "true");
        System.setProperty("kylin.cache.redis.sentinel-enabled", "true");
        System.setProperty("kylin.cache.redis.sentinel-master", "master");

        Assertions.assertThrows(KylinException.class, () -> queryCacheManager.init());

        System.clearProperty("kylin.cache.redis.enabled");
        System.clearProperty("kylin.cache.redis.sentinel-enabled");
        System.clearProperty("kylin.cache.redis.sentinel-master");
    }

    @Test
    void testRecoverCache() {
        System.setProperty("kylin.cache.redis.enabled", "true");
        try {
            queryCacheManager.recoverCache();
        } catch (Exception e) {
            Assertions.fail();
        }
        System.clearProperty("kylin.cache.redis.enabled");
    }

    @Test
    void testRecoverCacheRedisCache() {
        System.setProperty("kylin.cache.redis.enabled", "true");
        kylinCache = Mockito.mock(RedisCache.class);
        ReflectionTestUtils.setField(queryCacheManager, "kylinCache", kylinCache);

        try {
            queryCacheManager.recoverCache();
        } catch (Exception e) {
            Assertions.fail();
        }

        System.clearProperty("kylin.cache.redis.enabled");
    }

    @Test
    void testRecoverCacheRedisCacheV2() {
        System.setProperty("kylin.cache.redis.enabled", "true");
        kylinCache = Mockito.mock(RedisCacheV2.class);
        ReflectionTestUtils.setField(queryCacheManager, "kylinCache", kylinCache);

        try {
            queryCacheManager.recoverCache();
        } catch (Exception e) {
            Assertions.fail();
        }

        System.clearProperty("kylin.cache.redis.enabled");
    }
}
