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

import org.mybatis.dynamic.sql.SqlColumn;

import java.sql.JDBCType;

public class LayoutDetailsDynamicSqlSupport {

    public static final LayoutDetails sqlTable = new LayoutDetails();

    private LayoutDetailsDynamicSqlSupport() {
    }

    public static final class LayoutDetails extends BasicSqlTable<LayoutDetailsDynamicSqlSupport.LayoutDetails> {

        public final SqlColumn<String> dataFlowId = column("dataflow_id", JDBCType.VARCHAR);

        public final SqlColumn<String> layoutId = column("layout_id", JDBCType.VARCHAR);

        public LayoutDetails() {
            super("layout_details", LayoutDetails::new);
        }
    }
}