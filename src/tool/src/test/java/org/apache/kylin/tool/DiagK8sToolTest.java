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

import static org.apache.kylin.common.util.TestUtils.getTestConfig;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.query.QueryHistoryInfo;
import org.apache.kylin.metadata.query.QueryMetrics;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpHeaders;

import io.kyligence.kap.engine.spark.job.NSparkSnapshotJob;

@MetadataInfo
class DiagK8sToolTest {

    private static String DEFAULT_PROJECT = "default";

    @TempDir
    File temporaryFolder;

    @Test
    void extractSysDiagTest(TestInfo testInfo) throws IOException {
        File mainDir = new File(temporaryFolder, testInfo.getDisplayName());
        FileUtils.forceMkdir(mainDir);

        DiagK8sTool tool = new DiagK8sTool(new HttpHeaders(), "full");
        tool.execute(new String[] { "-destDir", mainDir.getAbsolutePath() });

        File diagFile = mainDir.listFiles()[0].listFiles()[0];
        Assert.assertTrue(diagFile.exists() && diagFile.getName().endsWith(".zip"));

        List<String> contentInZip = readFromZip(diagFile);
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/metadata/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/audit_log/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/rec_candidate/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/job_info/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/favorite_rule/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/async_task/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/query_history_offset/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/conf/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/logs/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/spark_logs/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("info")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("kylin_env")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("time_used_info")));
    }

    @Test
    void extractQueryDiagTest(TestInfo testInfo) throws IOException {
        getTestConfig().setMetadataUrl(
                "test@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1;MODE=MYSQL,username=sa,password=");
        RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();
        String queryId = RandomUtil.randomUUIDStr();
        QueryMetrics queryMetrics = new QueryMetrics(queryId);
        QueryHistoryInfo queryHistoryInfo = new QueryHistoryInfo(true, 5, true);
        queryMetrics.setProjectName("default");
        queryMetrics.setQueryHistoryInfo(queryHistoryInfo);
        queryHistoryDAO.insert(queryMetrics);

        File mainDir = new File(temporaryFolder, testInfo.getDisplayName());
        FileUtils.forceMkdir(mainDir);

        DiagK8sTool tool = new DiagK8sTool(new HttpHeaders(), "query");
        tool.execute(new String[] { "-project", "default", "-query", queryId, "-destDir", mainDir.getAbsolutePath() });

        File diagFile = mainDir.listFiles()[0].listFiles()[0];
        Assert.assertTrue(diagFile.exists() && diagFile.getName().endsWith(".zip"));

        List<String> contentInZip = readFromZip(diagFile);
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/metadata/")));
        Assert.assertFalse(contentInZip.stream().anyMatch(c -> c.contains("/audit_log/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/query_history_offset/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/conf/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/logs/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/spark_logs/")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("info")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("kylin_env")));
        Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("time_used_info")));
    }

    @Test
    void extractJobDiagTest(TestInfo testInfo) throws IOException {
        try {
            KylinConfig config = getTestConfig();

            JobContextUtil.cleanUp();
            JobContextUtil.getJobContext(config);

            NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, DEFAULT_PROJECT);
            NSparkSnapshotJob job = NSparkSnapshotJob.create(tableManager.listAllTables().get(0), "ADMIN",
                    JobTypeEnum.SNAPSHOT_BUILD, RandomUtil.randomUUIDStr(), null, "false", null);
            ExecutableManager execMgr = ExecutableManager.getInstance(config, DEFAULT_PROJECT);
            execMgr.addJob(job);

            File mainDir = new File(temporaryFolder, testInfo.getDisplayName());
            FileUtils.forceMkdir(mainDir);

            DiagK8sTool tool = new DiagK8sTool(new HttpHeaders(), "job");
            tool.execute(new String[] { "-job", job.getId(), "-destDir", mainDir.getAbsolutePath() });

            File diagFile = mainDir.listFiles()[0].listFiles()[0];
            Assert.assertTrue(diagFile.exists() && diagFile.getName().endsWith(".zip"));

            List<String> contentInZip = readFromZip(diagFile);
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/metadata/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/audit_log/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/job_info/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/conf/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/logs/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.contains("/spark_logs/")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("info")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("kylin_env")));
            Assert.assertTrue(contentInZip.stream().anyMatch(c -> c.endsWith("time_used_info")));
        } finally {
            JobContextUtil.cleanUp();
        }

    }

    public List<String> readFromZip(File file) throws IOException {
        List<String> result = new ArrayList<>();
        try (ZipInputStream zip = new ZipInputStream(new FileInputStream(file))) {
            ZipEntry entry = null;
            while ((entry = zip.getNextEntry()) != null) {
                result.add(entry.getName());
            }
        }
        return result;
    }

}
