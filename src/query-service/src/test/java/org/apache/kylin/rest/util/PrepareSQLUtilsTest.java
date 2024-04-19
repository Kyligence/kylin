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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.query.engine.PrepareSqlStateParam;
import org.apache.kylin.query.util.PrepareSQLUtils;
import org.junit.Assert;
import org.junit.Test;

public class PrepareSQLUtilsTest {

    void verifyPrepareResult(String prepareSQL, String[] paramValues, String expectedResult) {
        PrepareSqlStateParam[] params = new PrepareSqlStateParam[paramValues.length];
        for (int i = 0; i < paramValues.length; i++) {
            params[i] = new PrepareSqlStateParam(String.class.getCanonicalName(), paramValues[i]);
        }
        verifyPrepareResult(prepareSQL, params, expectedResult);
    }

    void verifyPrepareResult(String prepareSQL, PrepareSqlStateParam[] params, String expectedResult) {
        Assert.assertEquals(expectedResult, PrepareSQLUtils.fillInParams(prepareSQL, params));
    }

    @Test
    public void testPrepareSQL() {
        verifyPrepareResult("select a from b where c = ? and d = ?", new String[] { "123", "d'2019-01-01'" },
                "select a from b where c = '123' and d = 'd'2019-01-01''");
        verifyPrepareResult("select \"a\" from \"b\" where \"c\" = ? and \"e\" = 'abc' and d = ?;",
                new String[] { "123", "d'2019-01-01'" },
                "select \"a\" from \"b\" where \"c\" = '123' and \"e\" = 'abc' and d = 'd'2019-01-01'';");
        verifyPrepareResult(
                "select * from (select \"a\", '?' as q from \"b\" where \"c\" = ? and \"e\" = 'abc' and d = ?) join (select \"b\" from z where x = ?)",
                new String[] { "123", "d'2019-01-01'", "abcdef" },
                "select * from (select \"a\", '?' as q from \"b\" where \"c\" = '123' and \"e\" = 'abc' and d = 'd'2019-01-01'') join (select \"b\" from z where x = 'abcdef')");
        verifyPrepareResult("select a from b where c = ? and d = ? and e = ? and f = ? and g = ? and h = ? and i = ? and j = ? and k = ?",
                new PrepareSqlStateParam[] {
                        new PrepareSqlStateParam(Integer.class.getCanonicalName(), "123"),
                        new PrepareSqlStateParam(Double.class.getCanonicalName(), "123.0"),
                        new PrepareSqlStateParam(String.class.getCanonicalName(), "a string"),
                        new PrepareSqlStateParam(Date.class.getCanonicalName(), "2019-01-01"),
                        new PrepareSqlStateParam(Timestamp.class.getCanonicalName(),
                                "2019-01-01 00:12:34.123"),
                        new PrepareSqlStateParam(Short.class.getCanonicalName(), "-128"),
                        new PrepareSqlStateParam(Long.class.getCanonicalName(), "-2147483648"),
                        new PrepareSqlStateParam(Boolean.class.getCanonicalName(), "true"),
                        new PrepareSqlStateParam(BigDecimal.class.getCanonicalName(), "-9223372036854775"),
                },
                "select a from b where c = 123 and d = 123.0 and e = 'a string' and f = date'2019-01-01' and g = timestamp'2019-01-01 00:12:34.123' "
                        + "and h = -128 and i = -2147483648 and j = true and k = -9223372036854775");
    }

}
