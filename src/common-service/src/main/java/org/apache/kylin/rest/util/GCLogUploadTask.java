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
package org.apache.kylin.rest.util;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.tool.util.ToolUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GCLogUploadTask implements Runnable {

    private static final int NUMBER_OF_GC_LOG_FILES = 10;
    private static final String UPLOAD_ERROR = "Upload GC log failed.";
    private final FileSystem fs = HadoopUtil.getWorkingFileSystem();
    private final File logDir;
    private final Path remoteDir;

    public GCLogUploadTask() {
        this(new File(KylinConfig.getKylinHome(), "logs"));
    }

    public GCLogUploadTask(File path) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        logDir = path;
        remoteDir = new Path(kylinConfig.getHdfsWorkingDirectory(), "_logs/" + AddressUtil.getHostName() + "/");
        init();
    }

    public void init() {
        try {
            if (!fs.exists(remoteDir)) {
                fs.mkdirs(remoteDir);
            }
        } catch (IOException e) {
            log.error("Failed to init logs folder at local and remote, ", e);
        }
    }

    @Override
    public void run() {
        log.trace("Start upload GC log");
        try {
            String gcLogPrefix = "kylin.gc.pid" + ToolUtil.getKylinPid() + ".";
            String currentGCLogSuffix = ".current";

            File[] gcLogs = logDir.listFiles(((dir, name) -> name.startsWith(gcLogPrefix)));
            FileStatus[] remoteGClogs = fs.listStatus(remoteDir, path -> path.getName().startsWith(gcLogPrefix));
            if (null == gcLogs || gcLogs.length == 0) {
                log.error(UPLOAD_ERROR + " Cannot find GC log for pid: " + ToolUtil.getKylinPid());
                return;
            }

            int localGcNum = -1;
            int remoteGcNum = -1;
            for (File gcLog : gcLogs) {
                String fileName = gcLog.getName();
                if (fileName.endsWith(currentGCLogSuffix)) {
                    localGcNum = Integer.parseInt(fileName.split("\\.")[3]);
                    break;
                }
            }
            if (localGcNum == -1) {
                log.error(UPLOAD_ERROR + " Cannot find current GC log!");
                return;
            }
            for (FileStatus gcLog : remoteGClogs) {
                String fileName = gcLog.getPath().getName();
                if (fileName.endsWith(currentGCLogSuffix)) {
                    remoteGcNum = Integer.parseInt(fileName.split("\\.")[3]);
                    break;
                }
            }
            if (remoteGcNum != -1) {
                fs.delete(new Path(remoteDir, gcLogPrefix + remoteGcNum + currentGCLogSuffix), false);
            } else {
                remoteGcNum = 0;
            }
            if (localGcNum < remoteGcNum) {
                localGcNum += NUMBER_OF_GC_LOG_FILES;
            }

            for (int cur = remoteGcNum; cur <= localGcNum; cur++) {
                int realCur = cur % NUMBER_OF_GC_LOG_FILES;
                String curFileName;
                if (cur == localGcNum) {
                    curFileName = gcLogPrefix + realCur + currentGCLogSuffix;
                    String toDeleteFileName = gcLogPrefix + realCur;
                    if (fs.exists(new Path(remoteDir, toDeleteFileName))) {
                        fs.delete(new Path(remoteDir, toDeleteFileName), false);
                    }
                } else {
                    curFileName = gcLogPrefix + realCur;
                }
                HadoopUtil.uploadFileToHdfs(new File(logDir, curFileName), remoteDir);
            }

            log.trace("Upload GC log successful");
        } catch (Exception e) {
            log.error(UPLOAD_ERROR, e);
        }
    }
}
