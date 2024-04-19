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
package org.apache.kylin.tool;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import io.kyligence.kap.metadata.favorite.AsyncTaskManager;

public class AsyncTaskTool {
    private static final Logger logger = LoggerFactory.getLogger("diag");

    public void backup() {
        // TODO
    }

    public void restore() {
        // TODO
    }

    public void extractFull(File dir) throws IOException {
        logger.info("Extract async task.");
        List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();
        for (ProjectInstance project : projects) {
            extractProject(dir, project.getName());
        }
    }

    public void extractProject(File dir, String project) throws IOException {
        logger.info("Extract async task for project: {}", project);
        File projectFile = new File(dir, project);
        AsyncTaskManager manager = AsyncTaskManager.getInstance(project);
        try (OutputStream os = new FileOutputStream(projectFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (String taskType : AsyncTaskManager.ALL_TASK_TYPES) {
                AbstractAsyncTask task = manager.get(taskType);
                try {
                    bw.write(JsonUtil.writeValueAsString(task));
                    bw.newLine();
                } catch (Exception e) {
                    logger.error("Write error, id is {}", task.getId(), e);
                }
            }
        }
    }
}
