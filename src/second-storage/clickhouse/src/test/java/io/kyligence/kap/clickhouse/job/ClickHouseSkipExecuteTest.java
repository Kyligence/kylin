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

package io.kyligence.kap.clickhouse.job;

import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_ADD_SECONDARY_INDEX;
import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_LAYOUT_ID;
import static io.kyligence.kap.clickhouse.job.ClickhouseRefreshSecondaryIndex.CLICKHOUSE_REMOVE_SECONDARY_INDEX;
import static org.mockito.ArgumentMatchers.any;

import org.apache.commons.lang3.RandomUtils;
import org.apache.kylin.common.extension.KylinInfoExtension;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import lombok.val;
import lombok.var;

public class ClickHouseSkipExecuteTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testClickHouseIndexCleanDoWork() throws ExecuteException {
        testClickHouseIndexCleanDoWork(ClickHouseIndexClean.class);
        testClickHouseIndexCleanDoWork(ClickHousePartitionClean.class);

    }

    private void testClickHouseIndexCleanDoWork(Class<? extends AbstractClickHouseClean> clazz)
            throws ExecuteException {
        try (val mockStatic = Mockito.mockStatic(KylinInfoExtension.class)) {
            val clickhouseStep = Mockito.spy(clazz);
            Mockito.doReturn(ExecuteResult.createSucceed()).when(clickhouseStep).wrapWithExecuteException(any());
            val factory = Mockito.mock(KylinInfoExtension.Factory.class);
            Mockito.when(factory.checkKylinInfo()).thenReturn(true);
            mockStatic.when(KylinInfoExtension::getFactory).thenReturn(factory);
            val context = Mockito.mock(JobContext.class);
            var executeResult = clickhouseStep.doWork(context);
            Assert.assertTrue(executeResult.succeed());

            Mockito.when(factory.checkKylinInfo()).thenReturn(false);
            executeResult = clickhouseStep.doWork(context);
            Assert.assertTrue(executeResult.succeed());
        }
    }

    @Test
    public void testClickhouseRefreshSecondaryIndexDoWork() throws Exception {
        try (val mockStatic = Mockito.mockStatic(KylinInfoExtension.class)) {
            val clickhouseStep = Mockito.spy(ClickhouseRefreshSecondaryIndex.class);
            Mockito.doReturn(ExecuteResult.createSucceed()).when(clickhouseStep).wrapWithExecuteException(any());
            Mockito.doReturn(RandomUtil.randomUUIDStr()).when(clickhouseStep).getTargetSubject();
            Mockito.doReturn(String.valueOf(RandomUtils.nextLong())).when(clickhouseStep)
                    .getParam(CLICKHOUSE_LAYOUT_ID);
            Mockito.doReturn(JsonUtil.writeValueAsString(Sets.newHashSet(RandomUtils.nextInt()))).when(clickhouseStep)
                    .getParam(CLICKHOUSE_ADD_SECONDARY_INDEX);
            Mockito.doReturn(JsonUtil.writeValueAsString(Sets.newHashSet(RandomUtils.nextInt()))).when(clickhouseStep)
                    .getParam(CLICKHOUSE_REMOVE_SECONDARY_INDEX);

            val factory = Mockito.mock(KylinInfoExtension.Factory.class);
            Mockito.when(factory.checkKylinInfo()).thenReturn(true);
            mockStatic.when(KylinInfoExtension::getFactory).thenReturn(factory);
            val context = Mockito.mock(JobContext.class);
            var executeResult = clickhouseStep.doWork(context);
            Assert.assertTrue(executeResult.succeed());

            Mockito.when(factory.checkKylinInfo()).thenReturn(false);
            executeResult = clickhouseStep.doWork(context);
            Assert.assertTrue(executeResult.skip());
        }
    }
}
