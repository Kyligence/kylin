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
import org.mybatis.dynamic.sql.BasicColumn;
import org.mybatis.dynamic.sql.SqlBuilder;
import org.mybatis.dynamic.sql.delete.render.DeleteStatementProvider;
import org.mybatis.dynamic.sql.insert.render.InsertStatementProvider;
import org.mybatis.dynamic.sql.render.RenderingStrategies;
import org.mybatis.dynamic.sql.select.render.SelectStatementProvider;
import org.mybatis.dynamic.sql.update.render.UpdateStatementProvider;
import org.mybatis.spring.SqlSessionTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import lombok.Getter;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class QueryHistoryIdOffsetStore {
    public static final String QUERY_HISTORY_OFFSET = "_query_history_offset";

    private final QueryHistoryIdOffsetTable table;
    @Getter
    private final SqlSessionTemplate sqlSessionTemplate;
    @Getter
    private final DataSourceTransactionManager transactionManager;

    public QueryHistoryIdOffsetStore(KylinConfig config) throws Exception {
        this(config, genQueryHistoryIdOffsetTableName(config));
    }

    private QueryHistoryIdOffsetStore(KylinConfig config, String tableName) throws Exception {
        StorageURL url = config.getCoreMetadataDBUrl();
        Properties props = JdbcUtil.datasourceParameters(url);
        DataSource dataSource = JdbcDataSource.getDataSource(props);
        table = new QueryHistoryIdOffsetTable(tableName);
        transactionManager = JdbcDataSource.getTransactionManager(dataSource);
        sqlSessionTemplate = new SqlSessionTemplate(
                QueryHistoryIdOffsetUtil.getSqlSessionFactory(dataSource, table.tableNameAtRuntime()));
    }

    private static String genQueryHistoryIdOffsetTableName(KylinConfig config) {
        StorageURL url = config.getCoreMetadataDBUrl();
        String tablePrefix = config.isUTEnv() ? "test_opt" : url.getIdentifier();
        return tablePrefix + QUERY_HISTORY_OFFSET;
    }

    public void save(QueryHistoryIdOffset offset) {
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        InsertStatementProvider<QueryHistoryIdOffset> insertStatement = getInsertProvider(offset);
        int rows = mapper.insert(insertStatement);
        if (rows > 0) {
            log.debug("Insert one query history offset for project ({}) into database.", offset.getProject());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                    "Failed to insert query history offset for project (%s)", offset.getProject()));
        }
    }

    public void update(QueryHistoryIdOffset offset) {
        // no need to update type and create_time
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        UpdateStatementProvider updateStatement = getUpdateProvider(offset);
        int rows = mapper.update(updateStatement);
        if (rows > 0) {
            log.debug("Update one query history offset for project ({})", offset.getProject());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                    "Failed to update query history project for project (%s)", offset.getProject()));
        }
    }

    public void updateWithoutCheckMvcc(QueryHistoryIdOffset offset) {
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        UpdateStatementProvider updateStatement = getUpdateWithoutCheckMvccProvider(offset);
        int rows = mapper.update(updateStatement);
        if (rows > 0) {
            log.debug("Update one query history offset for project ({})", offset.getProject());
        } else {
            throw new KylinException(FAILED_UPDATE_METADATA, String.format(Locale.ROOT,
                    "Failed to update query history project for project (%s)", offset.getProject()));
        }
    }

    public QueryHistoryIdOffset queryByProject(String project, String type) {
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        SelectStatementProvider statementProvider = getSelectByProjectStatementProvider(project, type);
        return mapper.selectOne(statementProvider);
    }

    public List<QueryHistoryIdOffset> queryAll() {
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        SelectStatementProvider statementProvider = select(getSelectFields(table)) //
                .from(table) //
                .build().render(RenderingStrategies.MYBATIS3);
        return mapper.selectMany(statementProvider);
    }

    public void deleteByProject(String project) {
        QueryHistoryIdOffsetMapper mapper = sqlSessionTemplate.getMapper(QueryHistoryIdOffsetMapper.class);
        DeleteStatementProvider deleteStatement = SqlBuilder.deleteFrom(table)//
                .where(table.project, SqlBuilder.isEqualTo(project)) //
                .build().render(RenderingStrategies.MYBATIS3);
        mapper.delete(deleteStatement);
        log.info("Delete query history offset for project ({})", project);
    }

    InsertStatementProvider<QueryHistoryIdOffset> getInsertProvider(QueryHistoryIdOffset offset) {
        var provider = SqlBuilder.insert(offset).into(table);
        return provider.map(table.project).toProperty("project") //
                .map(table.type).toProperty("type") //
                .map(table.offset).toProperty("offset") //
                .map(table.updateTime).toProperty("updateTime") //
                .map(table.createTime).toProperty("createTime") //
                .map(table.mvcc).toProperty("mvcc") //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateProvider(QueryHistoryIdOffset offset) {
        return SqlBuilder.update(table)//
                .set(table.offset).equalTo(offset::getOffset) //
                .set(table.updateTime).equalTo(offset::getUpdateTime) //
                .set(table.mvcc).equalTo(offset.getMvcc() + 1) //
                .where(table.id, SqlBuilder.isEqualTo(offset::getId)) //
                .and(table.mvcc, SqlBuilder.isEqualTo(offset::getMvcc)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    UpdateStatementProvider getUpdateWithoutCheckMvccProvider(QueryHistoryIdOffset offset) {
        return SqlBuilder.update(table)//
                .set(table.offset).equalTo(offset::getOffset) //
                .set(table.updateTime).equalTo(offset::getUpdateTime) //
                .set(table.mvcc).equalTo(offset.getMvcc() + 1) //
                .where(table.project, SqlBuilder.isEqualTo(offset::getProject))
                .and(table.type, SqlBuilder.isEqualTo(offset::getType))//
                .build().render(RenderingStrategies.MYBATIS3);
    }

    SelectStatementProvider getSelectByProjectStatementProvider(String project, String type) {
        return select(getSelectFields(table)) //
                .from(table) //
                .where(table.project, isEqualTo(project)) //
                .and(table.type, isEqualTo(type)) //
                .build().render(RenderingStrategies.MYBATIS3);
    }

    private BasicColumn[] getSelectFields(QueryHistoryIdOffsetTable offsetTable) {
        return BasicColumn.columnList(//
                offsetTable.id, //
                offsetTable.project, //
                offsetTable.type, //
                offsetTable.offset, //
                offsetTable.updateTime, //
                offsetTable.createTime, //
                offsetTable.mvcc);
    }
}
