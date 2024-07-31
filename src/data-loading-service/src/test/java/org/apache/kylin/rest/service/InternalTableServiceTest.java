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

package org.apache.kylin.rest.service;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.engine.spark.NLocalWithSparkSessionTestBase;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.service.InternalTableLoadingService;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.response.InternalTableDescResponse;
import org.apache.kylin.rest.response.InternalTableLoadingJobResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.context.SecurityContextHolder;

@MetadataInfo
public class InternalTableServiceTest extends AbstractTestCase {

    @Mock
    private AclEvaluate aclEvaluate = Mockito.spy(AclEvaluate.class);
    @Spy
    private InternalTableLoadingService internalTableLoadingService = Mockito.spy(new InternalTableLoadingService());
    @InjectMocks
    private InternalTableService internalTableService = Mockito.spy(new InternalTableService());

    @InjectMocks
    private TableService tableService = mock(TableService.class);

    static final String PROJECT = "default";
    static final String TABLE_INDENTITY = "DEFAULT.TEST_KYLIN_FACT";
    static final String DATE_COL = "CAL_DT";
    static final String INTERNAL_DIR = PROJECT + "/Internal/" + TABLE_INDENTITY.replace(".", "/");
    static final String BASE_SQL = "select * from INTERNAL_CATALOG." + PROJECT + "." + TABLE_INDENTITY;

    @BeforeAll
    public static void beforeClass() {
        NLocalWithSparkSessionTestBase.beforeClass();
    }

    @AfterAll
    public static void afterClass() {
        NLocalWithSparkSessionTestBase.afterClass();
    }

    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
        SecurityContextHolder.getContext()
                .setAuthentication(new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN));
        SparkJobFactoryUtils.initJobFactory();
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
        when(tableService.getPartitionColumnFormat(any(), any(), any(), any())).thenReturn("yyyy-MM-dd");
    }

    @Test
    void testCheckParams() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        // null value is valid
        internalTableService.checkParameters(null, table, null);

        // empty array & blank string are valid too
        String[] partitionCols = new String[] {};
        String datePartitionFormat = "";
        internalTableService.checkParameters(partitionCols, table, datePartitionFormat);

        // partitionCols are case insensitive
        partitionCols = new String[] { "TRANS_ID", "order_id" };
        datePartitionFormat = "yyyy-MM-dd";
        internalTableService.checkParameters(partitionCols, table, datePartitionFormat);

        // when datePartitionFormat is null, non-date cols can be used as partitionCol
        internalTableService.checkParameters(partitionCols, table, "");

        // test partitionCols include date, but datePartitionFormat is null
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(new String[] { "CAL_DT" }, table, ""));

        // test invalid partitionCols
        Assertions.assertThrows(KylinException.class, () -> internalTableService
                .checkParameters(new String[] { "TRANS_ID", "order_id_2" }, table, "yyyy-MM-dd"));

        // test invalid partitionCols
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.checkParameters(new String[] { "TRANS_ID", "CAL_DT" }, table, "yyyy-mm"));

    }

    @Test
    void testCreateInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), partitionCols,
                "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNotNull(internalTable);

        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertTrue(internalTableFolder.exists() && internalTableFolder.isDirectory());

        // test create duplicated internal table
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test create internal table without tableDesc
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.createInternalTable(PROJECT, table.getName() + "_xxx", table.getDatabase(),
                        partitionCols, "yyyy-MM-dd", tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test drop internal table without data dir
        if (!internalTableFolder.delete()) {
            Assertions.fail();
        }
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
    }

    @Test
    void testUpdateInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), new String[] {}, null,
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getTablePartition());
        Assertions.assertTrue(internalTable.getTblProperties().isEmpty());

        String[] partitionCols = new String[] { DATE_COL };
        Map<String, String> tblProperties = new HashMap<>();
        tblProperties.put("orderByKeys", "LO_ORDERKEY");
        tblProperties.put("primaryKey", "LO_ORDERKEY2");
        String dateFormat = "yyyy-MM-dd";
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(),
                partitionCols, dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name());

        // check internal table metadata
        List<InternalTableDescResponse> internalTables = internalTableService.getTableList(PROJECT);
        Assertions.assertEquals(1, internalTables.size());
        InternalTableDescResponse response = internalTables.get(0);
        Assertions.assertEquals(DATE_COL, response.getTimePartitionCol());

        // check internal table details
        List<InternalTablePartitionDetail> details = internalTableService.getTableDetail(PROJECT,
                internalTable.getDatabase(), internalTable.getName());
        Assertions.assertNull(details);

        // test set partitionCols to null
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(), null,
                dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getPartitionColumns());

        // test set partitionCols to empty
        internalTableService.updateInternalTable(PROJECT, internalTable.getName(), internalTable.getDatabase(),
                new String[] {}, dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);
        Assertions.assertNull(internalTable.getPartitionColumns());

        // test update an internal table without create
        String db = internalTable.getDatabase();
        String tableName = internalTable.getName();
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.updateInternalTable(PROJECT, "TEST_ACCOUNT", db, partitionCols, dateFormat,
                        tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

        // test update an internal table which has loaded data.
        UnitOfWork.doInTransactionWithRetry(() -> {
            InternalTableManager manager = InternalTableManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
            manager.updateInternalTable(TABLE_INDENTITY, copyForWrite -> copyForWrite.setStorageSize(1L));
            return null;
        }, PROJECT);

        Assertions.assertThrows(TransactionException.class, () -> internalTableService.updateInternalTable(PROJECT,
                tableName, db, partitionCols, dateFormat, tblProperties, InternalTableDesc.StorageType.PARQUET.name()));

    }

    @Test
    void testReloadInternalTableSchema() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        try {
            internalTableService.reloadInternalTableSchema(PROJECT, TABLE_INDENTITY);
        } catch (Exception e) {
            Assertions.fail("Expect no exception.", e);
        }

        internalTableService.createInternalTable(PROJECT, table, InternalTableDesc.StorageType.PARQUET.name());
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        // mock file under internal table folder
        File tmpFile = new File(internalTableFolder, "mocked_file");
        boolean created = tmpFile.createNewFile();
        Assertions.assertTrue(created);
        Assertions.assertEquals(1, internalTableFolder.list().length);

        internalTableService.reloadInternalTableSchema(PROJECT, TABLE_INDENTITY);
        Assertions.assertEquals(0, internalTableFolder.list().length);
    }

    @Test
    void testLoadAndDeleteInternalTable() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        internalTableService.createInternalTable(PROJECT, table, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), false, false, "", "", null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertEquals(1, internalTableFolder.list().length);

        // check query
        SparkSession ss = SparderEnv.getSparkSession();
        Assertions.assertFalse(ss.sql(BASE_SQL).isEmpty());

        // check truncate
        response = internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY);
        jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);
        Assertions.assertEquals(0, internalTableFolder.list().length);

        // double truncate
        response = internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertTrue(response.getJobs().isEmpty());

        // test truncate nonexistent internal tables
        Assertions.assertThrows(KylinException.class,
                () -> internalTableService.truncateInternalTable(PROJECT, TABLE_INDENTITY + "2"));

        // check delete
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());

        // test drop an internal table twice
        Assertions.assertThrows(TransactionException.class,
                () -> internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY));
    }

    @Test
    void testLoadAndDeleteInternalTableWithIncremental() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();

        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        internalTableService.createInternalTable(PROJECT, TABLE_INDENTITY, new String[] { DATE_COL }, "yyyy-MM-dd",
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());
        String startDate = "1325347200000"; // 2012-01-01
        String endDate = "1325865600000"; // 2012-01-07
        InternalTableLoadingJobResponse response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(),
                table.getDatabase(), true, false, startDate, endDate, null);
        String jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);

        // check internal table data exist
        String workingDir = config.getHdfsWorkingDirectory().replace("file://", "");
        File internalTableFolder = new File(workingDir, INTERNAL_DIR);
        Assertions.assertEquals(6, internalTableFolder.list().length);

        // check query
        SparkSession ss = SparderEnv.getSparkSession();
        long count = ss.sql(BASE_SQL).count();
        Assertions.assertTrue(count > 0);

        // refresh some partitions and check agine
        String middleDate = "1325520000000";
        response = internalTableService.loadIntoInternalTable(PROJECT, table.getName(), table.getDatabase(), true, true,
                startDate, middleDate, null);
        jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);
        Assertions.assertEquals(count, ss.sql(BASE_SQL).count());

        // remove some partitions and check
        String[] toDeletePartitions = new String[] { "2012-01-03", "2012-01-04" };
        response = internalTableService.dropPartitionsOnDeltaTable(PROJECT, TABLE_INDENTITY, toDeletePartitions, null);
        jobId = response.getJobs().get(0).getJobId();
        waitJobToFinished(config, jobId);
        Assertions.assertEquals(6 - toDeletePartitions.length, internalTableFolder.list().length);
        long newCount = ss.sql(BASE_SQL).count();
        Assertions.assertTrue(newCount > 0 && newCount < count);

        // check delete table
        internalTableService.dropInternalTable(PROJECT, TABLE_INDENTITY);
        Assertions.assertFalse(internalTableFolder.exists());
    }

    private void waitJobToFinished(KylinConfig config, String jobId) {
        ExecutableManager executableManager = ExecutableManager.getInstance(config, PROJECT);
        await().atMost(5, TimeUnit.MINUTES).until(() -> {
            ExecutableState state = executableManager.getJob(jobId).getStatus();
            return state.isFinalState() || state == ExecutableState.ERROR;
        });
        // check job
        Assertions.assertEquals(ExecutableState.SUCCEED, executableManager.getJob(jobId).getStatus());
    }

    @Test
    void testGetTableListAndDetails() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);

        List<InternalTablePartitionDetail> details = internalTableService.getTableDetail(PROJECT, table.getDatabase(),
                table.getName());
        Assertions.assertNull(details);

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), null, null,
                new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());

        List<InternalTableDescResponse> tables = internalTableService.getTableList(PROJECT);
        Assertions.assertEquals(1, tables.size());
        Assertions.assertNull(tables.get(0).getTimePartitionCol());

        details = internalTableService.getTableDetail(PROJECT, table.getDatabase(), table.getName());
        Assertions.assertNull(details);

        internalTableService.updateInternalTable(PROJECT, tables.get(0).getTableName(), tables.get(0).getDatabaseName(),
                new String[] { DATE_COL }, "yyyy-MM-dd", new HashMap<>(), InternalTableDesc.StorageType.PARQUET.name());

        tables = internalTableService.getTableList(PROJECT);
        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals(DATE_COL, tables.get(0).getTimePartitionCol());

        details = internalTableService.getTableDetail(PROJECT, table.getDatabase(), table.getName());
        Assertions.assertNull(details);
    }

    @Test
    void testLoadWithTblProperties() throws Exception {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tManager = NTableMetadataManager.getInstance(config, PROJECT);
        InternalTableManager internalTableManager = InternalTableManager.getInstance(config, PROJECT);
        TableDesc table = tManager.getTableDesc(TABLE_INDENTITY);
        InternalTableLoader loader = new InternalTableLoader();
        SparkSession ss = SparderEnv.getSparkSession();

        HashMap<String, String> tblProperties = new HashMap<>();
        tblProperties.put("primaryKey", null);
        tblProperties.put("orderByKey", null);
        tblProperties.put("bucketCol", null);
        tblProperties.put("bucketNum", null);

        internalTableService.createInternalTable(PROJECT, table.getName(), table.getDatabase(), null, null,
                tblProperties, InternalTableDesc.StorageType.PARQUET.name());
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(TABLE_INDENTITY);

        // load without additional parameters
        loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);

        // load with bucketCol but without bucketNum
        tblProperties.put("bucketCol", DATE_COL);
        internalTable.setTblProperties(tblProperties);
        Assertions.assertThrows(KylinException.class,
                () -> loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false));

        // load with bucketCol && bucketNum
        tblProperties.put("bucketNum", "1");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }

        // load with orderByKey
        tblProperties.put("orderByKey", "TRAND_ID");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }
        // load with orderByKey && primaryKey
        tblProperties.put("primaryKey", "TRAND_ID");
        try {
            loader.loadInternalTable(ss, internalTable, "true", "0", "0", "default", false);
        } catch (Exception e) {
            Assertions.fail();
        }
    }
}
