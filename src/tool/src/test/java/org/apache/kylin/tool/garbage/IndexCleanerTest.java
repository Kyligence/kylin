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

package org.apache.kylin.tool.garbage;

import static org.awaitility.Awaitility.await;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.project.NProjectManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import lombok.var;

public class IndexCleanerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void init() {
        createTestMetadata();
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        projectManager.updateProject(DEFAULT_PROJECT, copyForWrite -> {
            var properties = copyForWrite.getOverrideKylinProps();
            if (properties == null) {
                properties = Maps.newLinkedHashMap();
            }
            properties.put("kylin.metadata.semi-automatic-mode", "true");
            copyForWrite.setOverrideKylinProps(properties);
        });
    }

    @After
    public void destroy() {
        RawRecManager recManager = RawRecManager.getInstance(DEFAULT_PROJECT);
        recManager.deleteByProject(DEFAULT_PROJECT);
        cleanupTestMetadata();
    }

    @Test
    public void testIndexCleanerCleanIndex() {
        String modelName = "nmodel_basic";
        KylinConfig config = getTestConfig();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
        NDataModel model = dataModelManager.getDataModelDescByAlias(modelName);
        RawRecManager recManager = RawRecManager.getInstance(DEFAULT_PROJECT);
        Assert.assertEquals(0, recManager.queryAll().size());
        Assert.assertEquals(0, model.getRecommendationsCount());

        cleanIndex();
        model = dataModelManager.getDataModelDescByAlias(modelName);
        Assert.assertEquals(1, recManager.queryAll().size());
        Assert.assertEquals(1, model.getRecommendationsCount());
    }

    @Test
    public void testParallelIndexCleaner() {
        String modelName = "nmodel_basic";
        KylinConfig config = getTestConfig();
        NDataModelManager dataModelManager = NDataModelManager.getInstance(config, DEFAULT_PROJECT);
        NDataModel model = dataModelManager.getDataModelDescByAlias(modelName);
        RawRecManager recManager = RawRecManager.getInstance(DEFAULT_PROJECT);
        Assert.assertEquals(0, recManager.queryAll().size());
        Assert.assertEquals(0, model.getRecommendationsCount());

        List<Thread> threads = new ArrayList<>();
        Runnable runnable = this::cleanIndex;
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.add(new Thread(runnable));
        threads.forEach(Thread::start);

        await().atMost(10, TimeUnit.SECONDS).until(() -> threads.stream().noneMatch(Thread::isAlive));

        model = dataModelManager.getDataModelDescByAlias(modelName);
        Assert.assertEquals(1, recManager.queryAll().size());
        Assert.assertEquals(1, model.getRecommendationsCount());
    }

    private void cleanIndex() {
        IndexCleaner indexCleaner = new IndexCleaner(DEFAULT_PROJECT, false);
        indexCleaner.prepare();
        UnitOfWork.doInTransactionWithRetry(() -> {
            indexCleaner.execute();
            return 0;
        }, DEFAULT_PROJECT);
    }
}
