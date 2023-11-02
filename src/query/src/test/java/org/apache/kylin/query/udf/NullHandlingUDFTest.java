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

package org.apache.kylin.query.udf;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.Date;
import java.sql.Timestamp;

import org.apache.kylin.query.udf.nullHandling.IsNullUDF;
import org.junit.Test;

public class NullHandlingUDFTest {

    @Test
    public void testIsNullUDF() throws Exception {
        IsNullUDF isNullUDF = new IsNullUDF();

        assertFalse(isNullUDF.ISNULL("Apache"));
        String str = null;
        assertTrue(isNullUDF.ISNULL(str));

        assertFalse(isNullUDF.ISNULL(2.3));
        Double d = null;
        assertTrue(isNullUDF.ISNULL(d));

        assertFalse(isNullUDF.ISNULL(2));
        Integer integer = null;
        assertTrue(isNullUDF.ISNULL(integer));

        Date date1 = new Date(System.currentTimeMillis());
        assertFalse(isNullUDF.ISNULL(date1));
        Date date2 = null;
        assertTrue(isNullUDF.ISNULL(date2));

        Timestamp timestamp1 = new Timestamp(System.currentTimeMillis());
        assertFalse(isNullUDF.ISNULL(timestamp1));
        Timestamp timestamp2 = null;
        assertTrue(isNullUDF.ISNULL(timestamp2));

        assertFalse(isNullUDF.ISNULL(true));
        Boolean b = null;
        assertTrue(isNullUDF.ISNULL(b));
    }
}
