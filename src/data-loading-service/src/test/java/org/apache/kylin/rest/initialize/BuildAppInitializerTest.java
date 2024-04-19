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

package org.apache.kylin.rest.initialize;

import java.io.File;
import java.io.IOException;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;

@MetadataInfo(onlyProps = true)
class BuildAppInitializerTest {

    @TempDir
    File temporaryFolder;

    @Test
    void checkLogPathTest(TestInfo testInfo) throws IOException {
        File logPath = new File(temporaryFolder, testInfo.getDisplayName());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.engine.spark-conf.spark.eventLog.dir", logPath.getAbsolutePath());

        Assert.assertFalse(logPath.exists());
        BuildAppInitializer.checkSparkLogPath();
        Assert.assertTrue(logPath.exists());
        BuildAppInitializer.checkSparkLogPath();
        Assert.assertTrue(logPath.exists());
    }

    @Test
    void checkLogPathWithEventLogDisabledTest(TestInfo testInfo) throws IOException {
        File logPath = new File(temporaryFolder, testInfo.getDisplayName());
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setProperty("kylin.engine.spark-conf.spark.eventLog.dir", logPath.getAbsolutePath());
        config.setProperty("kylin.engine.spark-conf.spark.eventLog.enabled", "false");

        Assert.assertFalse(logPath.exists());
        BuildAppInitializer.checkSparkLogPath();
        Assert.assertFalse(logPath.exists());
    }
}
