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
package org.apache.kylin.common.persistence.metadata.mapper;

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;

public final class TableInfoDynamicSqlSupport {

    public static final TableInfo sqlTable = new TableInfo();

    private TableInfoDynamicSqlSupport() {
    }

    public static final class TableInfo extends BasicSqlTable<TableInfo> {
        public final SqlColumn<String> name = column("name", JDBCType.VARCHAR);

        public final SqlColumn<String> tableIdentity = column("table_identity", JDBCType.VARCHAR);
        public TableInfo() {
            super("table_info", TableInfo::new);
        }
    }
}
