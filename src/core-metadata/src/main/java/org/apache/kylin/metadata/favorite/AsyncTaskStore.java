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

package org.apache.kylin.metadata.favorite;

import static org.apache.kylin.common.exception.CommonErrorCode.FAILED_UPDATE_METADATA;
import static org.mybatis.dynamic.sql.SqlBuilder.deleteFrom;
import static org.mybatis.dynamic.sql.SqlBuilder.isEqualTo;
import static org.mybatis.dynamic.sql.SqlBuilder.select;

import java.util.List;
import java.util.Locale;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.StorageURL;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.asynctask.AbstractAsyncTask;
import org.apache.kylin.util.MetadataStoreUtil;
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import lombok.Getter;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AsyncTaskStore {
    public static final String ASYNC_TASK = "_async_task";

    private final AsyncTaskTable table;
    @Getter
    private final SqlSessionTemplate sqlSessionTemplate;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    public AsyncTaskStore(KylinConfig config) throws Exception {
        this(config, genTableName(config));
    }

    private AsyncTaskStore(KylinConfig config, String tableName) throws Exception {
        StorageURL url = config.getCoreMetadataDBUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new AsyncTaskTable(tableName);
        transactionManager = new DataSourceTransactionManager(dataSource);
        sqlSessionTemplate = new SqlSessionTemplate(MetadataStoreUtil.getSqlSessionFactory(dataSource,
                table.tableNameAtRuntime(), MetadataStoreUtil.TableType.ASYNC_TASK));
    }

    private static String genTableName(KylinConfig config) {
        StorageURL url = config.getCoreMetadataDBUrl();
        String tablePrefix = config.isUTEnv() ? "test_opt" : url.getIdentifier();
        return tablePrefix + ASYNC_TASK;
    }

    public void save(AbstractAsyncTask task) {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        InsertStatementProvider<AbstractAsyncTask> insertStatement = getInsertProvider(task);
        int rows = mapper.insert(insertStatement);
        if (rows > 0) {
            log.debug("Insert one async task for project ({}) into database.", task.getProject());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA,
                    String.format(Locale.ROOT, "Failed to insert async task for project (%s)", task.getProject()));
        }
    }

    public void update(AbstractAsyncTask task) {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        UpdateStatementProvider updateStatement = getUpdateProvider(task);
        int rows = mapper.update(updateStatement);
        if (rows > 0) {
            log.debug("Update one async task for project ({})", task.getProject());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                    "Failed to update async task for project (%s)", task.getProject()));
        }
    }

    public List<AbstractAsyncTask> queryByType(String taskType) {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        SelectStatementProvider statementProvider = getSelectByTaskTypeStatementProvider(taskType);
        return mapper.selectMany(statementProvider);
    }

    public AbstractAsyncTask queryByTypeAndKey(String taskKey, String taskType) {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        SelectStatementProvider statementProvider = getSelectByTypeAndKeyStatementProvider(taskKey, taskType);
        return mapper.selectOne(statementProvider);
    }

    public List<AbstractAsyncTask> queryAll() {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                .from(table) //
                .build().render(RenderingStrategies.MYBATIS3);
        return mapper.selectMany(statementProvider);
    }

    public void deleteByProject(String project) {
        AsyncTaskMapper mapper = sqlSessionTemplate.getMapper(AsyncTaskMapper.class);
        DeleteStatementProvider deleteStatement = deleteFrom(table)//
                .where(table.project, isEqualTo(project)) //
                .build().render(RenderingStrategies.MYBATIS3);
        mapper.delete(deleteStatement);
        log.info("Delete favorite rule for project ({})", project);
    }

    InsertStatementProvider<AbstractAsyncTask> getInsertProvider(AbstractAsyncTask task) {
        var provider = SqlBuilder.insert(task).into(table);
        return provider.map(table.project).toProperty("project") //
                .map(table.taskAttributes).toProperty("taskAttributes") //
                .map(table.taskType).toProperty("taskType") //
                .map(table.taskKey).toProperty("taskKey") //
                .map(table.updateTime).toProperty("updateTime") //
                .map(table.createTime).toProperty("createTime") //
                .map(table.mvcc).toProperty("mvcc") //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateProvider(AbstractAsyncTask task) {

        return SqlBuilder.update(table) //
                .set(table.taskAttributes).equalTo(task::getTaskAttributes) //
                .set(table.taskType).equalTo(task::getTaskType) //
                .set(table.taskKey).equalTo(task::getTaskKey)
                .set(table.updateTime).equalTo(task::getUpdateTime) //
                .set(table.mvcc).equalTo(task.getMvcc() + 1) //
                .where(table.id, isEqualTo(task::getId)) //
                .and(table.mvcc, isEqualTo(task::getMvcc)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByTaskTypeStatementProvider(String taskType) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.taskType, isEqualTo(taskType)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByTypeAndKeyStatementProvider(String taskType, String taskKey) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.taskKey, isEqualTo(taskKey)) //
                .and(table.taskType, isEqualTo(taskType)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(AsyncTaskTable taskTable) {
        return BasicColumn.columnList(//
                taskTable.id, //
                taskTable.project, //
                taskTable.taskKey,
                taskTable.taskAttributes, //
                taskTable.taskType, //
                taskTable.updateTime, //
                taskTable.createTime, //
                taskTable.mvcc);
    }
}
