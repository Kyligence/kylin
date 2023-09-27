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

package org.apache.kylin.query.schema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.guava30.shaded.common.annotations.VisibleForTesting;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.TableDesc;

public class OlapSchema extends AbstractSchema {

    private KylinConfig config;
    private final String projectName;
    private final String schemaName;
    private final List<TableDesc> tables;
    private final Map<String, List<NDataModel>> modelsMap;
    private String starSchemaUrl;
    private String starSchemaUser;
    private String starSchemaPassword;

    public OlapSchema(String project, String schemaName, List<TableDesc> tables,
            Map<String, List<NDataModel>> modelsMap) {
        this.projectName = project;
        this.schemaName = schemaName;
        this.tables = tables;
        this.modelsMap = modelsMap;
        init();
    }

    private void init() {
        this.config = KylinConfig.getInstanceFromEnv();
        this.starSchemaUrl = config.getHiveUrl();
        this.starSchemaUser = config.getHiveUser();
        this.starSchemaPassword = config.getHivePassword();
    }

    /**
     * It is intended to skip caching, because underlying project/tables might change.
     */
    @Override
    public Map<String, Table> getTableMap() {
        return createTableMap();
    }

    public boolean hasTables() {
        return tables != null && !tables.isEmpty();
    }

    private Map<String, Table> createTableMap() {
        Map<String, Table> olapTables = new HashMap<>();

        for (TableDesc tableDesc : tables) {
            // safe to use tableDesc.getUuid() here, it is in a DB context now
            final String tableName = tableDesc.getName();
            final OlapTable table = new OlapTable(this, tableDesc, modelsMap);
            olapTables.put(tableName, table);
        }
        return olapTables;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public boolean hasStarSchemaUrl() {
        return starSchemaUrl != null && !starSchemaUrl.isEmpty();
    }

    public String getStarSchemaUrl() {
        return starSchemaUrl;
    }

    public String getStarSchemaUser() {
        return starSchemaUser;
    }

    public String getStarSchemaPassword() {
        return starSchemaPassword;
    }

    public NDataModelManager getMetadataManager() {
        return NDataModelManager.getInstance(config, projectName);
    }

    public KylinConfig getConfig() {
        return config;
    }

    @VisibleForTesting
    public void setConfigOnlyInTest(KylinConfig config) {
        this.config = config;
    }

    public String getProjectName() {
        return this.projectName;
    }
}
