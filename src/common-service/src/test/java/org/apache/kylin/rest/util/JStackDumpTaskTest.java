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
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.AddressUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JStackDumpTaskTest extends NLocalFileMetadataTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testExecute() throws IOException {
        getTestConfig().setProperty("kylin.task.upload-jstack-dump-enabled", "true");
        getTestConfig().setProperty("kylin.task.jstack-dump-log-files-max-count", "1");

        File mainDir = new File(temporaryFolder.getRoot(), "JStackDumpTask");
        FileUtils.forceMkdir(mainDir);

        JStackDumpTask task = new JStackDumpTask(mainDir);
        task.run();
        Assert.assertTrue(Arrays.stream(mainDir.listFiles()).anyMatch(x -> x.getName().contains("jstack.timed.log")));

        Path logsOnWorkingDir = new Path(getTestConfig().getHdfsWorkingDirectory(),
                "_logs/" + AddressUtil.getHostName());
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FileStatus[] fileStatuses = fs.listStatus(logsOnWorkingDir);
        Assert.assertEquals(1, fileStatuses.length);
        Assert.assertTrue(fileStatuses[0].getPath().getName().contains("jstack.timed.log"));
        task.run();
        FileStatus[] fileStatuses2 = fs.listStatus(logsOnWorkingDir);
        Assert.assertEquals(1, fileStatuses2.length);
        Assert.assertTrue(fileStatuses2[0].getPath().getName().contains("jstack.timed.log"));
        Assert.assertNotEquals(fileStatuses[0].getPath().getName(), fileStatuses2[0].getPath().getName());
    }
}
