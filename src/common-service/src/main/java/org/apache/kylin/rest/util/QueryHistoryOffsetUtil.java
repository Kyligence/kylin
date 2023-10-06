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

package org.apache.kylin.rest.util;

import static io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset.OffsetType.ACCELERATE;
import static io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset.OffsetType.META;

import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.metadata.query.RDBMSQueryHistoryDAO;

import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffset;
import io.kyligence.kap.metadata.favorite.QueryHistoryIdOffsetManager;

public class QueryHistoryOffsetUtil {

    private static RDBMSQueryHistoryDAO queryHistoryDAO = RDBMSQueryHistoryDAO.getInstance();

    public static void resetOffsetId(String project) {
        resetOffsetId(project, queryHistoryDAO);
    }

    public static void resetOffsetId(String project, RDBMSQueryHistoryDAO queryHistoryDAO) {
        QueryHistoryIdOffsetManager qhIdOffsetManager = QueryHistoryIdOffsetManager.getInstance(project);
        JdbcUtil.withTxAndRetry(qhIdOffsetManager.getTransactionManager(), () -> {
            long maxId = queryHistoryDAO.getQueryHistoryMaxId(project);
            QueryHistoryIdOffsetManager manager = QueryHistoryIdOffsetManager.getInstance(project);
            QueryHistoryIdOffset queryHistoryAccIdOffset = manager.get(ACCELERATE);
            QueryHistoryIdOffset queryHistoryStatIdOffset = manager.get(META);
            if (queryHistoryAccIdOffset.getOffset() > maxId || queryHistoryStatIdOffset.getOffset() > maxId) {
                manager.updateOffset(ACCELERATE, copyForWrite -> {
                    copyForWrite.setOffset(maxId);
                });
                manager.updateOffset(META, copyForWrite -> {
                    copyForWrite.setOffset(maxId);
                });
            }
            return null;
        });
    }

}
