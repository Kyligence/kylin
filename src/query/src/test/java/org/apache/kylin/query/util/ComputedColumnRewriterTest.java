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

package org.apache.kylin.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.junit.annotation.MetadataInfo;
import org.junit.jupiter.api.Test;

@MetadataInfo
class ComputedColumnRewriterTest {

    @Test
    void testGetRexNodeStrWithPlusAndTimes() throws SqlParseException {

        String expected = "*(ITEM_COUNT, ROUND(*(+(PRICE, 11), 12), 0))";
        {
            String sql = "select (round((F.PRICE + 11) * 12, 0)) * F.ITEM_COUNT from test_kylin_fact F";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }

        {
            String sql = "select (round(12 * (F.PRICE + 11), 0)) * F.ITEM_COUNT from test_kylin_fact F";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }

        {
            String sql = "select (round(12 * ( 11+F.PRICE), 0)) * F.ITEM_COUNT from test_kylin_fact F";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }

        {
            String sql = "select  F.ITEM_COUNT * (round(12 * ( 11+F.PRICE), 0))  from test_kylin_fact F";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }
    }

    @Test
    void testGetRexNodeStrWithCaseWhen() throws SqlParseException {
        String expected = "CASE(<(100, PRICE), *(*(PRICE, ITEM_COUNT), 2), CAST(*(PRICE, ITEM_COUNT)):DECIMAL(38, 4))";
        {
            String sql = "select case when price > 100 then price * item_count * 2 else price * item_count end from test_kylin_fact";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }

        {
            String sql = "select case when 100 < price then item_count * price * 2  else price * item_count end from test_kylin_fact";
            String rexStr = ComputedColumnRewriter.getRexNodeStr(KylinConfig.getInstanceFromEnv(), "default", sql);
            assertEquals(expected, rexStr);
        }
    }
}
