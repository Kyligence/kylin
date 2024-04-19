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

package org.apache.kylin.rest;

import static org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil.isTableExists;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Properties;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.session.SessionProperties;
import org.springframework.boot.autoconfigure.session.StoreType;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.security.util.InMemoryResource;
import org.springframework.session.web.context.AbstractHttpSessionApplicationInitializer;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "spring.session.store-type", havingValue = "JDBC")
public class SessionConfig extends AbstractHttpSessionApplicationInitializer {
    private static final String CREATE_SCHEMA_SESSION_TABLE = "create.schema-session.table";
    private static final String CREATE_SCHEMA_SESSION_ATTRIBUTES_TABLE = "create.schema-session-attributes.table";

    @Autowired
    @Qualifier
    DataSource dataSource;

    @Autowired
    SessionProperties sessionProperties;

    private void initSessionTable(String replaceName, String sessionScript) throws IOException {
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();

        sessionScript = sessionScript.replaceAll("SPRING_SESSION", replaceName);
        populator.addScript(new InMemoryResource(sessionScript));
        populator.setContinueOnError(false);
        DatabasePopulatorUtils.execute(populator, dataSource);
    }

    @PostConstruct
    public void initSessionTables() throws SQLException, IOException {
        if (sessionProperties.getStoreType() != StoreType.JDBC) {
            return;
        }

        String tableName = KylinConfig.getInstanceFromEnv().getMetadataUrlPrefix() + "_session_v2";
        // mysql table name is case sensitive, sql template is using capital letters.
        String attributesTableName = tableName + "_ATTRIBUTES";

        Properties properties = JdbcUtil.getProperties((BasicDataSource) dataSource);

        if (!isTableExists(dataSource.getConnection(), tableName)) {
            initSessionTable(tableName, properties.getProperty(CREATE_SCHEMA_SESSION_TABLE));
        }

        if (!isTableExists(dataSource.getConnection(), attributesTableName)) {
            initSessionTable(tableName, properties.getProperty(CREATE_SCHEMA_SESSION_ATTRIBUTES_TABLE));
        }
    }

}
