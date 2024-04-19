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

package org.apache.kylin.mapper;

import java.nio.charset.Charset;
import java.util.List;

import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.job.JobContext;
import org.apache.kylin.job.config.JobTableInterceptor;
import org.apache.kylin.job.constant.JobStatusEnum;
import org.apache.kylin.job.domain.JobInfo;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.mapper.JobInfoMapper;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.rest.util.SpringContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@Ignore
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(value = { "classpath:applicationContext.xml" })
@TestPropertySource(properties = { "spring.cloud.nacos.discovery.enabled = false" })
@TestPropertySource(properties = { "spring.session.store-type = NONE" })
public class JobInfoMapperTest extends NLocalFileMetadataTestCase {

    @Autowired
    private JobContext jobContext;

    @BeforeClass
    public static void setupClass() {
        staticCreateTestMetadata();
        // change kylin.env to load bean
        getTestConfig().setProperty("kylin.env", "testing");
    }

    @Before
    public void setup() {
        // add mybatis intercepter, (this applicationContext is not mybatis-SpringBoot, should config yourself)
        SqlSessionFactory sqlSessionFactory = (SqlSessionFactory) SpringContext.getBean("sqlSessionFactory");
        JobTableInterceptor jobTableInterceptor = SpringContext.getBean(JobTableInterceptor.class);
        sqlSessionFactory.getConfiguration().addInterceptor(jobTableInterceptor);

    }

    private JobInfo generateJobInfo() {

        JobInfo jobInfo = new JobInfo();
        jobInfo.setJobId("mock_job_id");
        jobInfo.setJobType(JobTypeEnum.INDEX_BUILD.name());
        jobInfo.setJobStatus(ExecutableState.READY.name());
        jobInfo.setProject("mock_project");
        jobInfo.setSubject("mock_subject");
        jobInfo.setModelId("mock_model_id");
        jobInfo.setJobDurationMillis(0L);
        jobInfo.setJobContent("mock_job_content".getBytes(Charset.forName("UTF-8")));

        return jobInfo;
    }

    @Test
    public void jobInfoCrud() {
        JobInfoMapper jobInfoMapper = jobContext.getJobInfoMapper();

        // Create

        JobInfo jobInfo = generateJobInfo();

        int insertAffect = jobInfoMapper.insert(jobInfo);
        Assert.assertEquals(1, insertAffect);

        // Read
        JobInfo mockJob = jobInfoMapper.selectByJobId("mock_job_id");
        Assert.assertEquals("mock_job_id", mockJob.getJobId());

        List<String> jobLists = jobInfoMapper.findJobIdListByStatusBatch(ExecutableState.READY.name(), 10);
        Assert.assertEquals(1, jobLists.size());

        JobMapperFilter jobMapperFilter = JobMapperFilter.builder().jobId("mock_job_id").build();
        List<JobInfo> jobInfos = jobInfoMapper.selectByJobFilter(jobMapperFilter);
        Assert.assertEquals(1, jobLists.size());

        long countLong = jobInfoMapper.countByJobFilter(jobMapperFilter);
        Assert.assertEquals(1L, countLong);

        countLong = jobInfoMapper.countByJobFilter(JobMapperFilter.builder().jobId("mock_job_id_not_existing").build());
        Assert.assertEquals(0L, countLong);

        // update
        JobInfo jobInfoDb = jobInfos.get(0);
        jobInfoDb.setJobStatus(JobStatusEnum.DISCARDED.name());
        int updateAffect = jobInfoMapper.updateByJobIdSelective(jobInfoDb);
        Assert.assertEquals(1, updateAffect);
        mockJob = jobInfoMapper.selectByJobId("mock_job_id");
        Assert.assertEquals(JobStatusEnum.DISCARDED.name(), mockJob.getJobStatus());

        // delete
        int deleteAffect = jobInfoMapper.deleteByJobId("mock_job_id");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        jobInfoMapper.deleteByProject("mock_project");
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobInfoMapper.deleteByJobIdList(Lists.newArrayList(ExecutableState.READY.name()),
                Lists.newArrayList("mock_job_id"));
        Assert.assertEquals(1, deleteAffect);

        insertAffect = jobInfoMapper.insertJobInfoSelective(jobInfo);
        Assert.assertEquals(1, insertAffect);
        deleteAffect = jobInfoMapper.deleteAllJob();
        Assert.assertEquals(1, deleteAffect);

    }

}
