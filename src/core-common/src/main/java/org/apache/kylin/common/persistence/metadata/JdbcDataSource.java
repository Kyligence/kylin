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
package org.apache.kylin.common.persistence.metadata;

import java.util.Collection;
import java.util.Map;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSourceFactory;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

public class JdbcDataSource {
    private JdbcDataSource() {
    }

    private static Map<Properties, DataSource> instances = Maps.newConcurrentMap();

    private static Map<DataSource, DataSourceTransactionManager> tmManagerMap = Maps.newConcurrentMap();

    public static DataSource getDataSource(Properties props) throws Exception {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return BasicDataSourceFactory.createDataSource(props);
        }
        if (instances.containsKey(props)) {
            return instances.get(props);
        }
        instances.putIfAbsent(props, BasicDataSourceFactory.createDataSource(props));
        return instances.get(props);
    }

    public static DataSourceTransactionManager getTransactionManager(DataSource dataSource) {
        if (KylinConfig.getInstanceFromEnv().isUTEnv()) {
            return new DataSourceTransactionManager(dataSource);
        }
        if (tmManagerMap.containsKey(dataSource)) {
            return tmManagerMap.get(dataSource);
        }
        tmManagerMap.putIfAbsent(dataSource, new DataSourceTransactionManager(dataSource));
        return tmManagerMap.get(dataSource);
    }

    public static DataSourceTransactionManager getTransactionManager(Properties props) throws Exception {
        DataSource dataSource = getDataSource(props);
        return getTransactionManager(dataSource);
    }

    public static Collection<DataSource> getDataSources() {
        return instances.values();
    }
}
