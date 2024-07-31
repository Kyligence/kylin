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

import static org.apache.kylin.common.exception.ServerErrorCode.EMPTY_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_ERROR;
import static org.apache.kylin.common.exception.ServerErrorCode.INTERNAL_TABLE_NOT_EXIST;
import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_INTERNAL_TABLE_PARAMETER;
import static org.apache.kylin.common.exception.ServerErrorCode.TABLE_NOT_EXIST;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.engine.spark.builder.InternalTableLoader;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.job.execution.ExecutableManager;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.service.InternalTableLoadingService;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.table.InternalTableDesc;
import org.apache.kylin.metadata.table.InternalTableManager;
import org.apache.kylin.metadata.table.InternalTablePartition;
import org.apache.kylin.metadata.table.InternalTablePartitionDetail;
import org.apache.kylin.rest.response.InternalTableDescResponse;
import org.apache.kylin.rest.response.InternalTableLoadingJobResponse;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.util.DataRangeUtils;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import scala.Option;

@Service("internalTableService")
public class InternalTableService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(InternalTableService.class);

    @Autowired
    private AclEvaluate aclEvaluate;

    @Autowired
    private InternalTableLoadingService internalTableLoadingService;

    @Autowired
    private TableService tableService;

    /**
     * Create an internal table from an existing table
     * @param projectName
     * @param table
     * @param database
     * @param partitionCols
     * @param datePartitionFormat
     * @param tblProperties
     * @throws Exception
     */
    public void createInternalTable(String projectName, String table, String database, String[] partitionCols,
            String datePartitionFormat, Map<String, String> tblProperties, String storageType) throws Exception {
        String tableIdentity = database + "." + table;
        createInternalTable(projectName, tableIdentity, partitionCols, datePartitionFormat, tblProperties, storageType);
    }

    public void createInternalTable(String projectName, String tableIdentity, String[] partitionCols,
            String datePartitionFormat, Map<String, String> tblProperties, String storageType) throws Exception {
        aclEvaluate.checkProjectWritePermission(projectName);

        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, projectName);
            InternalTableManager internalTableManager = getManager(InternalTableManager.class, projectName);
            Map<String, String> properties = Maps.newHashMap();
            properties.putAll(tblProperties);
            TableDesc originTable = tableMetadataManager.getTableDesc(tableIdentity);
            if (Objects.isNull(originTable)) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getTableNotFound(), tableIdentity);
                throw new KylinException(TABLE_NOT_EXIST, errorMsg);
            }
            if (originTable.getHasInternal()) {
                throw new KylinException(INTERNAL_TABLE_ERROR, "Table is already an internal table");
            }
            checkParameters(partitionCols, originTable, datePartitionFormat);
            InternalTableDesc internalTable = new InternalTableDesc(originTable);
            createInternalTablePath(internalTable.generateInternalTableLocation());
            if (partitionCols != null && partitionCols.length != 0) {
                InternalTablePartition tablePartition = new InternalTablePartition(partitionCols, datePartitionFormat);
                internalTable.setTablePartition(tablePartition);
            }
            internalTable.setTblProperties(properties);
            internalTable.optimizeTblProperties();
            internalTable.setStorageType(storageType);
            internalTable.setLocation(internalTable.generateInternalTableLocation());
            tableMetadataManager.updateTableDesc(originTable.getIdentity(),
                    copyForWrite -> copyForWrite.setHasInternal(true));
            createDeltaSchema(internalTable);
            internalTableManager.saveOrUpdateInternalTable(internalTable);
            return true;
        }, projectName);
    }

    public void checkParameters(String[] partitionCols, TableDesc originTable, String datePartitionFormat)
            throws Exception {
        if (!Objects.isNull(partitionCols)) {
            List<ColumnDesc> partitionColList = Arrays.stream(partitionCols)
                    .map(col -> originTable.findColumnByName(col)).filter(col -> col != null)
                    .collect(Collectors.toList());
            // exist unmatched partition columns
            if (partitionCols.length != partitionColList.size()) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getPartitionColumnNotExist(),
                        originTable.getIdentity());
                throw new KylinException(INVALID_INTERNAL_TABLE_PARAMETER, errorMsg);
            }
            Optional<ColumnDesc> dateCol = partitionColList.stream().filter(col -> col.getTypeName().equals("date"))
                    .findFirst();
            if (StringUtils.isEmpty(datePartitionFormat) && dateCol.isPresent()) {
                throw new KylinException(EMPTY_PARAMETER, "date_partition_format can not be null, please check again");
            }
            // detect date partition format
            if (dateCol.isPresent() && !StringUtils.isEmpty(datePartitionFormat)
                    && !tableService.getPartitionColumnFormat(originTable.getProject(), originTable.getIdentity(),
                            dateCol.get().getName(), null).equals(datePartitionFormat)) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getIncorrectDateformat(),
                        datePartitionFormat);
                throw new KylinException(INVALID_INTERNAL_TABLE_PARAMETER, errorMsg);

            }
        }
    }

    public void createDeltaSchema(InternalTableDesc internalTable) throws IOException {
        if (internalTable.getStorageType() == InternalTableDesc.StorageType.GLUTEN) {
            Option<SparkSession> defaultSession = SparkSession.getDefaultSession();
            InternalTableLoader internalTableLoader = new InternalTableLoader();
            internalTableLoader.onlyLoadSchema(true);
            internalTableLoader.loadInternalTable(defaultSession.get(), internalTable, "true", "", "",
                    KylinConfig.getInstanceFromEnv().getGlutenStoragePolicy(), false);
        }
    }

    /**
     * create an internal table with source table
     * and no partition columns and table properties specified
     * @param project
     * @param originTable
     * @param storageType
     */
    public void createInternalTable(String project, TableDesc originTable, String storageType) throws Exception {
        createInternalTable(project, originTable.getName(), originTable.getDatabase(), null, null, new HashMap<>(),
                storageType);
    }

    public void updateInternalTable(String project, String table, String database, String[] partitionCols,
            String datePartitionFormat, Map<String, String> tblProperties, String storageType) {
        aclEvaluate.checkProjectWritePermission(project);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            String dbTblName = database + "." + table;
            NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
            InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
            TableDesc originTable = tableMetadataManager.getTableDesc(dbTblName);
            InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(dbTblName);
            if (Objects.isNull(internalTable)) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableNotFound(), dbTblName);
                throw new KylinException(INTERNAL_TABLE_NOT_EXIST, errorMsg);
            }
            if (internalTable.getStorageSize() > 0L) {
                throw new KylinException(INTERNAL_TABLE_ERROR, "Loaded internal table can not be updated");
            }
            checkParameters(partitionCols, originTable, datePartitionFormat);
            if (partitionCols != null && partitionCols.length != 0) {
                InternalTablePartition tablePartition = new InternalTablePartition(partitionCols, datePartitionFormat);
                internalTable.setTablePartition(tablePartition);
            } else {
                internalTable.setTablePartition(null);
            }
            internalTable.setTblProperties(tblProperties);
            internalTable.optimizeTblProperties();
            internalTable.setStorageType(storageType);
            deleteMetaAndDataInFileSystem(internalTable);
            createDeltaSchema(internalTable);
            internalTableManager.saveOrUpdateInternalTable(internalTable);
            return true;
        }, project);
    }

    protected void createInternalTablePath(String path) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path location = new Path(path);
            fs.mkdirs(location);
        } catch (IOException e) {
            throw new KylinException(INTERNAL_TABLE_ERROR, "Failed to create internal table location", e);
        }
    }

    protected void deleteMetaAndDataInFileSystem(InternalTableDesc internalTable) {
        try {
            FileSystem fs = HadoopUtil.getWorkingFileSystem();
            Path location = new Path(internalTable.getLocation());
            if (fs.exists(location)) {
                HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(), location);
                logger.info("Successfully deleted internal table on {}", internalTable.getLocation());
            }
        } catch (IOException e) {
            logger.error("Failed to delete internal table on {}", internalTable.getLocation(), e);
        }
    }

    public void suicideRunningInternalTableJob(String project, String table) {
        try {
            ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), project).suicideRunningJobByJobType(project,
                    table,
                    Lists.newArrayList(JobTypeEnum.INTERNAL_TABLE_BUILD.name(),
                            JobTypeEnum.INTERNAL_TABLE_REFRESH.name(),
                            JobTypeEnum.INTERNAL_TABLE_DELETE_PARTITION.name()));
        } catch (Exception e) {
            logger.warn("Failed to suicide running internal table job for table {}", table, e);
        }
    }

    // 1. delete data in filesystem
    // 2. clear internal table metadata
    // 3. change mark in source table
    public void dropInternalTable(String project, String tableIdentity) {
        aclEvaluate.checkProjectWritePermission(project);
        suicideRunningInternalTableJob(project, tableIdentity);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager tableMetadataManager = getManager(NTableMetadataManager.class, project);
            InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
            InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableIdentity);
            if (Objects.isNull(internalTable)) {
                String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableNotFound(),
                        tableIdentity);
                throw new KylinException(TABLE_NOT_EXIST, errorMsg);
            }
            tableMetadataManager.updateTableDesc(tableIdentity, copyForWrite -> copyForWrite.setHasInternal(false));
            internalTableManager.removeInternalTable(tableIdentity);
            deleteMetaAndDataInFileSystem(internalTable);
            return true;
        }, project);

    }

    // 1. delete data in file system
    // 2. clear partition values in internal table meta
    public InternalTableLoadingJobResponse truncateInternalTable(String project, String tableIdentity)
            throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableIdentity);
        if (Objects.isNull(internalTable)) {
            String errorMsg = String.format(Locale.ROOT, MsgPicker.getMsg().getInternalTableNotFound(), tableIdentity);
            throw new KylinException(INTERNAL_TABLE_NOT_EXIST, errorMsg);
        }
        suicideRunningInternalTableJob(project, tableIdentity);
        if (internalTable.getRowCount() == 0) {
            logger.info("{} not have any data, skip truncate", tableIdentity);
            return InternalTableLoadingJobResponse.of(Collections.emptyList(), "");
        }
        return dropAllPartitionsOnDeltaTable(project, tableIdentity);
    }

    // 1. update delta log
    // 2. delete unused data files
    private InternalTableLoadingJobResponse dropAllPartitionsOnDeltaTable(String project, String tableIdentity) {
        return dropPartitionsOnDeltaTable(project, tableIdentity, new String[0], null);
    }

    // 1. delete partition data in file system
    // 2. update partition values in internal table meta
    // we shall do this delete action by a spark job and call delta delete api
    // so that the delta meta could be updated!
    public InternalTableLoadingJobResponse dropPartitionsOnDeltaTable(String project, String tableIdentity,
            String[] partitionValues, String yarnQueue) {
        aclEvaluate.checkProjectWritePermission(project);
        return internalTableLoadingService.dropPartitions(project, partitionValues, tableIdentity, yarnQueue);
    }

    // TODO need fix
    public void reloadInternalTableSchema(String project, String tableIdentity) throws Exception {
        aclEvaluate.checkProjectWritePermission(project);
        InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
        InternalTableDesc internalTable = internalTableManager.getInternalTableDesc(tableIdentity);
        if (internalTable != null) {
            dropInternalTable(project, tableIdentity);
            createInternalTable(project, tableIdentity, internalTable.getPartitionColumns(),
                    internalTable.getDatePartitionFormat(), internalTable.getTblProperties(),
                    internalTable.getStorageType().toString());
        }
    }

    /**
     *
     * @param project
     * @param table
     * @param isIncremental
     * @param startDate
     * @param endDate
     * @param yarnQueue if not null, use hadoop yarn resource to build, else use spark standalone
     * @return
     * @throws Exception
     */
    public InternalTableLoadingJobResponse loadIntoInternalTable(String project, String table, String database,
            boolean isIncremental, boolean isRefresh, String startDate, String endDate, String yarnQueue) {
        aclEvaluate.checkProjectWritePermission(project);
        if (isIncremental) {
            DataRangeUtils.validateRange(startDate, endDate);
        }
        return internalTableLoadingService.loadIntoInternalTable(project, table, database, isIncremental, isRefresh,
                startDate, endDate, yarnQueue);
    }

    public List<InternalTableDescResponse> getTableList(String project) {
        InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
        List<InternalTableDesc> tableList = internalTableManager.listAllTables();
        List<InternalTableDescResponse> descList = Lists.newArrayList();
        tableList.forEach(internalTableDesc -> {
            InternalTableDescResponse internalTableDescResponse = new InternalTableDescResponse();
            internalTableDescResponse.setTableName(internalTableDesc.getName());
            internalTableDescResponse.setUuid(internalTableDesc.getUuid());
            internalTableDescResponse.setDatabaseName(internalTableDesc.getDatabase());
            internalTableDescResponse.setRowCount(internalTableDesc.getRowCount());
            internalTableDescResponse.setStorageSize(internalTableDesc.getStorageSize());
            internalTableDescResponse.setHitCount(internalTableDesc.getHitCount());
            String[] partitionColumns = internalTableDesc.getPartitionColumns();
            String partitionColumn = (partitionColumns == null || partitionColumns.length == 0) ? null
                    : internalTableDesc.getPartitionColumns()[0];
            internalTableDescResponse.setTimePartitionCol(partitionColumn);
            internalTableDescResponse.setUpdateTime(internalTableDesc.getLastModified());
            internalTableDescResponse.setDatePartitionFormat(internalTableDesc.getDatePartitionFormat());
            descList.add(internalTableDescResponse);
        });
        return descList;
    }

    public List<InternalTablePartitionDetail> getTableDetail(String project, String databaseName, String tableName) {
        InternalTableManager internalTableManager = getManager(InternalTableManager.class, project);
        String tableIdentity = databaseName + "." + tableName;
        InternalTableDesc internalTableDesc = internalTableManager.getInternalTableDesc(tableIdentity);
        if (internalTableDesc == null || internalTableDesc.getTablePartition() == null) {
            return null;
        }
        return internalTableDesc.getTablePartition().getPartitionDetails();
    }

}
