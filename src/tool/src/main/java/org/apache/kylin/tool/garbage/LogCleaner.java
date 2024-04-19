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

import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.FileSystemUtil;
import org.apache.kylin.common.util.HadoopUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LogCleaner {

    public void cleanUp() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path remotePath = new Path(config.getHdfsWorkingDirectory(), "_logs");
        try {
            if (!fs.exists(remotePath) || fs.isFile(remotePath)) {
                log.info("Folder {} not exist, skip cleanUp.", remotePath);
                return;
            }
            for (FileStatus fileStatus : FileSystemUtil.listStatus(fs, remotePath)) {
                try {
                    if (System.currentTimeMillis() - fileStatus.getModificationTime() > config
                            .getInstanceLogRetentionTime()) {
                        fs.delete(fileStatus.getPath(), true);
                        log.info("Clean gc & jstack log for instance {} succeed.", fileStatus.getPath().getName());
                    }
                } catch (IOException e) {
                    log.error("Clean gc & jstack log for instance {} failed, skip this instance.",
                            fileStatus.getPath().getName());
                }
            }
        } catch (IOException e) {
            log.error("Check folder {} failed, skip cleanUp.", remotePath);
        }
    }
}
