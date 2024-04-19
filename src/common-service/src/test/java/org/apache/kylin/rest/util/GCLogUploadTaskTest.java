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

import static org.apache.kylin.common.util.TestUtils.writeToFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.tool.util.ToolUtil;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

@MetadataInfo(onlyProps = true)
class GCLogUploadTaskTest {

    @TempDir
    File temporaryFolder;

    @Test
    void testExecute() throws IOException {
        File mainDir = new File(temporaryFolder, "GCLogUploadTask");
        FileUtils.forceMkdir(mainDir);
        GCLogUploadTask task = new GCLogUploadTask(mainDir);
        String pid = ToolUtil.getKylinPid();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path logsOnWorkingDir = new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(),
                "_logs/" + AddressUtil.getHostName());

        File currentGCLog = new File(mainDir, "kylin.gc.pid" + pid + ".0.current");
        writeToFile(currentGCLog);
        task.run();
        checkGCLog(mainDir, logsOnWorkingDir, "kylin.gc.pid" + pid);

        rmCurrentSuffix(currentGCLog);
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".1"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".2"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".3"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".4"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".5"));
        File currentGCLog6 = new File(mainDir, "kylin.gc.pid" + pid + ".6.current");
        writeToFile(currentGCLog6);
        task.run();
        checkGCLog(mainDir, logsOnWorkingDir, "kylin.gc.pid" + pid);

        rmCurrentSuffix(currentGCLog6);
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".7"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".8"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".9"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".0"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".1"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".2"));
        addCurrentSuffix(new File(mainDir, "kylin.gc.pid" + pid + ".3"));
        writeToFile(new File(mainDir, "kylin.gc.pid" + pid + ".3.current"));
        task.run();
        checkGCLog(mainDir, logsOnWorkingDir, "kylin.gc.pid" + pid);
    }

    public void rmCurrentSuffix(File file) {
        file.renameTo(new File(file.getParent(), file.getName().substring(0, file.getName().lastIndexOf("."))));
    }

    public void addCurrentSuffix(File file) {
        file.renameTo(new File(file.getParent(), file.getName() + ".current"));
    }

    public void checkGCLog(File localDir, Path remoteDir, String gcLogPrefix) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            File[] localLogs = localDir.listFiles(((dir, name) -> name.startsWith(gcLogPrefix)));
            FileStatus[] remoteLogs = fs.listStatus(remoteDir, path -> path.getName().startsWith(gcLogPrefix));
            Assert.assertEquals(localLogs.length, remoteLogs.length);
            for (File file : localLogs) {
                Path remoteFile = new Path(remoteDir, file.getName());
                try (FileInputStream localIS = new FileInputStream(file);
                        FSDataInputStream remoteIS = fs.open(remoteFile)) {
                    Assert.assertTrue(IOUtils.contentEquals(localIS, remoteIS));
                }
            }
        } catch (IOException ignored) {
            Assert.fail();
        }
    }
}
