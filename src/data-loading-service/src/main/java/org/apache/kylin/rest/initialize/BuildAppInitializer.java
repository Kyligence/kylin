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

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.scheduler.EventBusFactory;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.rest.service.JobService;
import org.apache.kylin.rest.service.ModelBuildService;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class BuildAppInitializer implements InitializingBean {

    @Autowired
    private JobService jobService;

    @Autowired
    private ModelBuildService modelBuildService;

    @Override
    public void afterPropertiesSet() throws Exception {
        EventBusFactory.getInstance().registerService(jobService);
        EventBusFactory.getInstance().registerService(modelBuildService);
        checkSparkLogPath();
    }

    public static void checkSparkLogPath() throws IOException {
        Map<String, String> sparkConfigOverride = KylinConfig.getInstanceFromEnv().getSparkConfigOverride();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (sparkConfigOverride.containsKey("spark.eventLog.dir")) {
            String logDir = sparkConfigOverride.get("spark.eventLog.dir").trim();
            boolean eventLogEnabled = Boolean.parseBoolean(sparkConfigOverride.get("spark.eventLog.enabled"));
            if (eventLogEnabled && !logDir.isEmpty()) {
                Path logPath = new Path(logDir);
                if (!fs.exists(logPath)) {
                    fs.mkdirs(logPath);
                }
            }
        }
    }
}
