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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kylin.source.adhocquery.IPushDownConverter;

public class PowerBIConverter implements KapQueryUtil.IQueryTransformer, IPushDownConverter {

    private static final String S0 = "\\s*";
    private static final String SM = "\\s+";

    private static final Pattern PIN_SUM_OF_FN_CONVERT = Pattern.compile(
            S0 + "SUM" + S0 + "\\(" + S0 + "[{]" + S0 + "fn" + SM + "convert" + S0 + "\\(" + S0 + "([^\\s,]+)" + S0
                    + "," + S0 + "(SQL_DOUBLE|DOUBLE)" + S0 + "\\)" + S0 + "[}]" + S0 + "\\)",
            Pattern.CASE_INSENSITIVE);

    //Case: SUM({fn CONVERT(...)}) generated by PowerBI
    private static String handleSumOfFnConvert(String sql) {
        Matcher m;
        while (true) {
            m = PIN_SUM_OF_FN_CONVERT.matcher(sql);
            if (!m.find())
                break;

            sql = sql.substring(0, m.start()) + " SUM(" + m.group(1).trim() + ")"
                    + sql.substring(m.end(), sql.length());
        }
        return sql;
    }

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        return handleSumOfFnConvert(originSql);
    }

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        return handleSumOfFnConvert(sql);
    }
}
