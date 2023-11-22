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

package io.kyligence.kap.secondstorage.util;

import static org.junit.Assert.assertThrows;

import java.util.Collections;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ ExecutableManager.class })
@PowerMockIgnore({ "com.sun.security.*", "org.w3c.*", "javax.xml.*", "org.xml.*", "org.w3c.dom.*", "org.apache.cxf.*",
        "javax.management.*", "javax.script.*", "org.apache.hadoop.*", "javax.security.*", "java.security.*",
        "javax.crypto.*", "javax.net.ssl.*", "org.apache.kylin.common.asyncprofiler.AsyncProfiler" })
public class SecondStorageJobUtilTest extends NLocalFileMetadataTestCase {
    private ExecutableManager executableManager = Mockito.mock(ExecutableManager.class);

    @Before
    public void setUp() throws Exception {
        createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        cleanupTestMetadata();
    }

    private void prepareManger() {
        PowerMockito.stub(PowerMockito.method(ExecutableManager.class, "getInstance", KylinConfig.class, String.class))
                .toReturn(executableManager);
    }

    @Test
    public void testValidateModel() {
        prepareManger();
        PowerMockito.stub(PowerMockito.method(ExecutableManager.class, "getInstance", KylinConfig.class, String.class))
                .toReturn(executableManager);

        List<String> jobs = Collections.singletonList("job1");
        AbstractExecutable job1 = PowerMockito.mock(AbstractExecutable.class);
        PowerMockito.when(job1.getStatus()).thenReturn(ExecutableState.READY, ExecutableState.PENDING,
                ExecutableState.RUNNING, ExecutableState.PAUSED, ExecutableState.SUCCEED);

        PowerMockito.when(job1.getJobType()).thenReturn(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        Mockito.when(job1.getProject()).thenReturn("project");
        Mockito.when(job1.getTargetModelId()).thenReturn("modelId");
        Mockito.when(executableManager.getJobs()).thenReturn(jobs);
        Mockito.when(executableManager.getJob("job1")).thenReturn(job1);

        //job status: Ready
        assertThrows(MsgPicker.getMsg().getSecondStorageConcurrentOperate(), KylinException.class,
                () -> SecondStorageJobUtil.validateModel("project", "modelId"));
        //job status: Pending
        assertThrows(MsgPicker.getMsg().getSecondStorageConcurrentOperate(), KylinException.class,
                () -> SecondStorageJobUtil.validateModel("project", "modelId"));
        //job status: Running
        assertThrows(MsgPicker.getMsg().getSecondStorageConcurrentOperate(), KylinException.class,
                () -> SecondStorageJobUtil.validateModel("project", "modelId"));
        //job status: Paused
        assertThrows(MsgPicker.getMsg().getSecondStorageConcurrentOperate(), KylinException.class,
                () -> SecondStorageJobUtil.validateModel("project", "modelId"));
        //job status: Succeed
        SecondStorageJobUtil.validateModel("project", "modelId");
    }
}
