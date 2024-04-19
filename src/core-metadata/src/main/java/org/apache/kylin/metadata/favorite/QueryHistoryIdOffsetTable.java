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

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;
import org.mybatis.dynamic.sql.SqlTable;

public class QueryHistoryIdOffsetTable extends SqlTable {
    public final SqlColumn<Integer> id = column("id", JDBCType.INTEGER);
    public final SqlColumn<String> project = column("project", JDBCType.VARCHAR);
    public final SqlColumn<String> type = column("type", JDBCType.VARCHAR);
    public final SqlColumn<Long> offset = column("query_history_id_offset", JDBCType.BIGINT);
    public final SqlColumn<Long> updateTime = column("update_time", JDBCType.BIGINT);
    public final SqlColumn<Long> createTime = column("create_time", JDBCType.BIGINT);
    public final SqlColumn<Long> mvcc = column("mvcc", JDBCType.BIGINT);

    protected QueryHistoryIdOffsetTable(String tableName) {
        super(tableName);
    }
}
