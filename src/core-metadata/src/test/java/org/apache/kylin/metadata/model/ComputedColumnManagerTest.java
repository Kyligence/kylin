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
package org.apache.kylin.metadata.model;

import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.transaction.TransactionException;
import org.apache.kylin.common.persistence.transaction.UnitOfWork;
import org.apache.kylin.junit.annotation.JdbcMetadataInfo;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

@MetadataInfo(onlyProps = true)
@JdbcMetadataInfo
public class ComputedColumnManagerTest {

    @Test
    void testComputedColumnManager() {
        String project = "default";
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ComputedColumnManager manager = ComputedColumnManager.getInstance(config, project);
        Assertions.assertTrue(manager.listAll().isEmpty());

        ComputedColumnDesc cc1 = new ComputedColumnDesc();
        cc1.setProject(project);
        cc1.setColumnName("cc1");
        cc1.setTableIdentity("DB.T1");
        cc1.setExpression("T2.id + 1");
        ComputedColumnDesc cc2 = new ComputedColumnDesc();
        cc2.setProject(project);
        cc2.setColumnName("cc2");
        cc2.setTableIdentity("DB.T2");
        cc2.setExpression("T2.id + 1");
        UnitOfWork.doInTransactionWithRetry(() -> {
            ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.saveCCWithCheck(cc1);
            mgr.saveCCWithCheck(cc2);
            return true;
        }, project);
        Assertions.assertEquals(2, manager.listAll().size());

        // conflict with same expression
        ComputedColumnDesc cc3 = new ComputedColumnDesc();
        cc3.setProject("default");
        cc3.setColumnName("cc3");
        cc3.setTableIdentity("DB.T1");
        cc3.setExpression("T2.id + 1");
        Assertions.assertThrows(TransactionException.class, () -> {
            UnitOfWork.doInTransactionWithRetry(() -> {
                ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(),
                        project);
                mgr.saveCCWithCheck(cc3);
                return true;
            }, project);
        });

        // test with very long expression
        String longStr = Arrays.toString((new byte[2000]));
        ComputedColumnDesc cc4 = new ComputedColumnDesc();
        cc4.setProject("default");
        cc4.setColumnName("cc4");
        cc4.setTableIdentity("DB.T1");
        cc4.setExpression(longStr + "a");
        ComputedColumnDesc cc5 = new ComputedColumnDesc();
        cc5.setProject("default");
        cc5.setColumnName("cc5");
        cc5.setTableIdentity("DB.T1");
        cc5.setExpression(longStr + "b");
        UnitOfWork.doInTransactionWithRetry(() -> {
            ComputedColumnManager mgr = ComputedColumnManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.saveCCWithCheck(cc4);
            mgr.saveCCWithCheck(cc5);
            return true;
        }, project);
        Assertions.assertEquals(4, manager.listAll().size());
    }
}
