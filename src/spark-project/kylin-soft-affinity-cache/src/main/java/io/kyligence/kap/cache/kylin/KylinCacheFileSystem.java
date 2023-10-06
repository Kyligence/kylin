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
package io.kyligence.kap.cache.kylin;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.sql.SparkSession;

import io.kyligence.kap.cache.fs.AbstractCacheFileSystem;
import io.kyligence.kap.cache.fs.CacheFileSystemConstants;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinCacheFileSystem extends AbstractCacheFileSystem {

    /**
     * Check whether it needs to cache data on the current executor
     */
    @Override
    protected boolean isUseLocalCacheForCurrentExecutor() {
        if (null == TaskContext.get()) {
            log.warn("Task Context is null.");
            return false;
        }
        String localCacheForCurrExecutor = TaskContext.get()
                .getLocalProperty(CacheFileSystemConstants.PARAMS_KEY_LOCAL_CACHE_FOR_CURRENT_FILES);

        if (StringUtils.isBlank(localCacheForCurrExecutor))
            return true; // let the empty default be TRUE
        else
            return Boolean.parseBoolean(localCacheForCurrExecutor);
    }

    @Override
    protected long getAcceptCacheTime() {
        return getAcceptCacheTimeLocally();
    }

    /**
     * To instruct local cache eviction, sql can contain a hint like
     * <pre>
     *      select * from xxx /*+ ACCEPT_CACHE_TIME(158176387682000) * /    -- no space between * and /
     * </pre>
     * <p/>
     * The value is carried to Spark local property, naming "spark.kylin.local-cache.accept-cache-time",
     * and then is used by AbstractCacheFileSystem
     * <p/>
     * Returns [sql_without_cache_hint, accept_cache_time]
     */
    private static final Pattern PTN = Pattern.compile("[(]\\s*([0-9]+)");

    // returns [sql_with_hint_removed, the_millis_or_null]
    static String[] extractAcceptCacheTime(String sql) {
        int cut1 = sql.indexOf("ACCEPT_CACHE_TIME");
        if (cut1 < 0)
            return new String[] { sql, null };
        int cut2 = sql.lastIndexOf("/*", cut1);
        int cut3 = sql.indexOf("*/", cut1);
        if (cut2 < 0 || cut3 < 0)
            return new String[] { sql, null };

        String newSql = sql.substring(0, cut2) + sql.substring(cut3 + 2);
        String hintStr = sql.substring(cut2, cut3);
        String millis = null;

        Matcher m = PTN.matcher(hintStr);
        if (m.find()) {
            millis = m.group(1);
        }

        return new String[] { newSql, millis };
    }

    public static String processAcceptCacheTimeInSql(String sql) {
        String[] sqlAndAct = extractAcceptCacheTime(sql);
        setAcceptCacheTimeLocally(sqlAndAct[1]);
        return sqlAndAct[0];
    }

    public static void setAcceptCacheTimeLocally(String acceptCacheTime) {
        setAcceptCacheTimeLocally(
                acceptCacheTime == null ? System.currentTimeMillis() : Long.parseLong(acceptCacheTime));
    }

    /**
     * User can specify that any cache before the required time must be cleared.
     */
    public static void setAcceptCacheTimeLocally(long acceptCacheTime) {
        Preconditions.checkState(SparkSession.getDefaultSession().isDefined());
        SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
        String key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME;

        long now = System.currentTimeMillis();
        if (acceptCacheTime > now + 1000) { // +1000 for some error tolerance of time
            log.error("Accept-cache-time {} is later than clock time {}. Please check the time sync across machines.",
                    acceptCacheTime, now);
            acceptCacheTime = now;
        }

        // if set multiple times, take the max
        if (sparkContext.getLocalProperty(key) != null) {
            acceptCacheTime = Math.max(acceptCacheTime, Long.parseLong(sparkContext.getLocalProperty(key)));
        }

        sparkContext.setLocalProperty(key, Long.toString(acceptCacheTime));
    }

    public static void clearAcceptCacheTimeLocally() {
        Preconditions.checkState(SparkSession.getDefaultSession().isDefined());
        SparkContext sparkContext = SparkSession.getDefaultSession().get().sparkContext();
        sparkContext.setLocalProperty(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME, null);

        if (TaskContext.get() != null) { // should not have TaskContext, just in case..
            TaskContext.get().getLocalProperties().remove(CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME);
        }
    }

    public static long getAcceptCacheTimeLocally() {
        String key = CacheFileSystemConstants.PARAMS_KEY_ACCEPT_CACHE_TIME;

        // defaults to get latest data
        long ret = System.currentTimeMillis();

        if (TaskContext.get() != null) {
            String prop = TaskContext.get().getLocalProperty(key);
            if (prop != null)
                ret = Long.parseLong(prop);
        } else if (SparkSession.getDefaultSession().isDefined()) {
            SparkContext ctx = SparkSession.getDefaultSession().get().sparkContext();
            String prop = ctx.getLocalProperty(key);
            if (prop != null)
                ret = Long.parseLong(prop);
        }
        return ret;
    }
}
