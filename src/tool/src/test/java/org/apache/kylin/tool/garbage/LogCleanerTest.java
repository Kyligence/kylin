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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.Assert;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
public class LogCleanerTest {

    @Test
    void logsRetentionConfigTest() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Assert.assertEquals(7 * 24 * 60 * 60 * 1000, config.getInstanceLogRetentionTime());
        config.setProperty("kylin.log.gc-and-jstack.retention-time", "5s");
        Assert.assertEquals(5 * 1000, config.getInstanceLogRetentionTime());
    }

    @Test
    void cleanTest() throws IOException, InterruptedException {
        KylinConfig config = getTestConfig();
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path remoteDir = new Path(config.getHdfsWorkingDirectory(), "_logs");
        fs.mkdirs(remoteDir);

        config.setProperty("kylin.log.gc-and-jstack.retention-time", "3s");
        Path toDeletePath1 = new Path(remoteDir, "instance_1");
        Path toDeletePath2 = new Path(remoteDir, "instance_2");
        fs.mkdirs(toDeletePath1);
        fs.mkdirs(toDeletePath2);
        HadoopUtil.writeStringToHdfs("test", new Path(toDeletePath2, "gc.log"));
        HadoopUtil.writeStringToHdfs("test", new Path(toDeletePath2, "jstack.log"));

        TimeUnit.SECONDS.sleep(3);

        Path retentionPath1 = new Path(remoteDir, "instance_3");
        Path retentionPath2 = new Path(remoteDir, "instance_4");
        fs.mkdirs(retentionPath1);
        fs.mkdirs(retentionPath2);
        HadoopUtil.writeStringToHdfs("test", new Path(retentionPath2, "gc.log"));
        HadoopUtil.writeStringToHdfs("test", new Path(retentionPath2, "jstack.log"));

        new LogCleaner().cleanUp();
        Assert.assertFalse(fs.exists(toDeletePath1));
        Assert.assertFalse(fs.exists(toDeletePath2));
        Assert.assertTrue(fs.exists(retentionPath1));
        Assert.assertTrue(fs.exists(retentionPath2));
        Assert.assertEquals(2, fs.listStatus(retentionPath2).length);
    }
}
