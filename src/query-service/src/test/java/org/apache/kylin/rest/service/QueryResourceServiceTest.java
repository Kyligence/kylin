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

import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.spark.ExecutorAllocationClient;
import org.apache.spark.scheduler.ContainerInitializeListener;
import org.apache.spark.scheduler.ContainerSchedulerManager;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import lombok.val;
import lombok.var;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class QueryResourceServiceTest extends NLocalFileMetadataTestCase {

    private SparkSession ss;
    @InjectMocks
    private final QueryResourceService queryResourceService = Mockito.spy(new QueryResourceService());

    @Mock
    private ExecutorAllocationClient client;

    private ContainerSchedulerManager containerSchedulerManager;

    @BeforeClass
    public static void beforeClass() {
        if (SparderEnv.isSparkAvailable()) {
            SparderEnv.getSparkSession().close();
        }
        SparkSession.clearActiveSession();
        SparkSession.clearDefaultSession();
    }

    @Before
    public void setUp() throws Exception {
        System.setProperty("SPARK_LOCAL_IP", "localhost");
        MockitoAnnotations.initMocks(this);
        createTestMetadata();
        ss = SparkSession.builder().appName("local")
                .config("spark.extraListeners", "org.apache.spark.scheduler.ContainerInitializeListener")
                .master("local[1]").getOrCreate();
        SparderEnv.setSparkSession(ss);
        val sc = ss.sparkContext();
        containerSchedulerManager = new ContainerSchedulerManager(client, sc.listenerBus(), sc.conf(), sc.cleaner(),
                sc.resourceProfileManager());
        SparderEnv.setContainerSchedulerManager(containerSchedulerManager);
        Mockito.doReturn(true).when(client).isExecutorActive(Mockito.anyString());
        ss.range(1, 10).createOrReplaceTempView("queryResourceServiceTest");
        val data = ss.sql("SELECT id,count(0) FROM queryResourceServiceTest group by id");
        data.persist();
        data.show();
        data.unpersist();
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
        ss.stop();
    }

    @Test
    public void testContainerSchedulerManager() {
        Assert.assertEquals(0, ContainerInitializeListener.executorIds().size());
        Assert.assertTrue(queryResourceService.isAvailable());
        Assert.assertEquals("default", queryResourceService.getQueueName());
        String queue = "test_query_name";
        ss.sparkContext().conf().set("spark.kubernetes.executor.annotation.scheduling.volcano.sh/queue-name", queue);
        Assert.assertEquals(queue, queryResourceService.getQueueName());
        String queue2 = "test_query_name2";
        ss.sparkContext().conf().set("spark.kubernetes.executor.annotation.scheduling.kyligence.io.default-queue",
                queue2);
        Assert.assertEquals(queue2, queryResourceService.getQueueName());
        Assert.assertEquals(1, containerSchedulerManager.getAllExecutorCores());
        Assert.assertEquals(1408, containerSchedulerManager.getAllExecutorMemory());

        Assert.assertEquals(0, containerSchedulerManager.releaseExecutor(1, false).size());
        Assert.assertEquals(0, containerSchedulerManager.releaseExecutor(1, true).size());
    }

    @Test
    public void testAdjustQueryResource() {
        QueryResourceService.QueryResource queryResource = new QueryResourceService.QueryResource();
        queryResource.setCores(1);
        queryResource.setMemory(1023);
        var resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(0, resource.getCores());

        queryResource.setCores(1);
        queryResource.setMemory(1408);
        Mockito.doReturn(true).when(client).requestTotalExecutors(Mockito.any(), Mockito.any(), Mockito.any());
        resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(1, resource.getCores());

        queryResource.setCores(-1);
        queryResource.setMemory(1408);
        resource = queryResourceService.adjustQueryResource(queryResource);
        Assert.assertEquals(0, resource.getCores());
    }
}
