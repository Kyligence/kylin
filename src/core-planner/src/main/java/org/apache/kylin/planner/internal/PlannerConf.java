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

package org.apache.kylin.planner.internal;

import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.planner.internal.config.ConfigBuilder;
import org.apache.kylin.planner.internal.config.ConfigEntry;
import org.apache.kylin.planner.internal.config.ConfigReader;
import org.apache.kylin.planner.internal.config.OptionalConfigEntry;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PlannerConf implements Serializable {

    private static final String VERSION_5_0_0 = "5.0.0";

    private static final Object plannerConfEntriesUpdateLock = new Object();

    private static final ThreadLocal<PlannerConf> existingConf = ThreadLocal.withInitial(() -> null);

    private static final ThreadLocal<PlannerConf> fallbackConf = ThreadLocal.withInitial(PlannerConf::new);

    private static final AtomicReference<Supplier<PlannerConf>> confGetter = new AtomicReference<>(fallbackConf::get);

    private static Map<String, ConfigEntry<?>> plannerConfEntries = Collections.emptyMap();

    protected final transient Map<String, String> settings = Collections.synchronizedMap(new HashMap<>());

    protected final transient ConfigReader reader = new ConfigReader(settings);

    public int countLimit() {
        return getConf(MAX_CUBOID_COUNT);
    }

    public int changeLimit() {
        return getConf(MAX_CUBOID_CHANGE_COUNT);
    }

    public long dataRangeStart() {
        return getConf(DATA_RANGE_START);
    }

    public long dataRangeEnd() {
        return getConf(DATA_RANGE_END);
    }

    public <T> void set(ConfigEntry<T> configEntry, T value) {
        setConfString(configEntry.getKey(), configEntry.stringConverter.convert(value));
    }

    public <T> void set(OptionalConfigEntry<T> configEntry, T value) {
        setConfString(configEntry.getKey(), configEntry.rawStringConverter.convert(value));
    }

    public void setConfString(String key, String value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null for key: " + key);
        ConfigEntry<?> entry = getConfigEntry(key);
        if (entry != null) {
            entry.convertValue(value);
        }
        setConfWithCheck(key, value);
    }

    protected void setConfWithCheck(String key, String value) {
        logDeprecationWarning(key);
        requireDefaultValueOfRemovedConf(key, value);
        settings.put(key, value);
    }

    public void remove(String key) {
        settings.remove(key);
    }

    private void logDeprecationWarning(String key) {
        // wip: to be continued
        Objects.requireNonNull(key);
    }

    private void requireDefaultValueOfRemovedConf(String key, String value) {
        // wip: to be continued
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
    }

    public <T> T getConf(ConfigEntry<T> entry) {
        if (containsConfigEntry(entry)) {
            return entry.readFrom(reader);
        }
        throw new IllegalStateException(entry + " is not registered");
    }

    public static PlannerConf get() {
        PlannerConf conf = existingConf.get();
        if (conf == null) {
            return confGetter.get().get();
        }
        return conf;
    }

    public static void mergeKylinConf(PlannerConf plannerConf, KylinConfig kylinConf) {
        // wip: extract planner conf from kylin configs, to be continued.
        Objects.requireNonNull(plannerConf);
        Objects.requireNonNull(kylinConf);
    }

    public static void mergeParamConfigs(PlannerConf plannerConf, Map<String, String> confMap) {
        confMap.forEach(plannerConf::setConfString);
    }

    public static void clear() {
        existingConf.remove();
        fallbackConf.remove();
    }

    public static final ConfigEntry<Integer> INITIALIZE_CUBOID_COUNT = buildConf("kylin.planner.initializeCuboidCount")
            .internal().doc("Index planner initialize cuboid count.").version(VERSION_5_0_0).intConf()
            .createWithDefault(20);

    public static final ConfigEntry<Integer> MAX_CUBOID_COUNT = buildConf("kylin.planner.maxCuboidCount").internal()
            .doc("Index planner max cuboid count.").version(VERSION_5_0_0).intConf().createWithDefault(100);

    public static final ConfigEntry<Integer> MAX_CUBOID_CHANGE_COUNT = buildConf("kylin.planner.maxCuboidChangeCount")
            .internal().doc("Index planner max cuboid change count at once.").version(VERSION_5_0_0).intConf()
            .createWithDefault(10);

    public static final ConfigEntry<Long> DATA_RANGE_START = buildConf("kylin.planner.dataRangeStart").internal()
            .doc("Index planner data range start timestamp (inclusive).").version(VERSION_5_0_0).longConf()
            .createWithDefault(0L);

    public static final ConfigEntry<Long> DATA_RANGE_END = buildConf("kylin.planner.dataRangeEnd").internal()
            .doc("Index planner data range end timestamp (exclusive).").version(VERSION_5_0_0).longConf()
            .createWithDefault(
                    LocalDate.now(ZoneId.systemDefault()).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    public static final ConfigEntry<Boolean> AUTO_APPROVE_ENABLED = buildConf("kylin.planner.autoApproveEnabled")
            .internal().doc("Index planner enable auto approval.").version(VERSION_5_0_0).booleanConf()
            .createWithDefault(false);

    public static final OptionalConfigEntry<String> OPERATION_TOKEN = buildConf("kylin.planner.operationToken")
            .internal().doc("Index planner operation token.").version(VERSION_5_0_0).stringConf().createOptional();

    public static void register(ConfigEntry<?> entry) {
        synchronized (plannerConfEntriesUpdateLock) {
            if (plannerConfEntries.containsKey(entry.getKey())) {
                throw new IllegalStateException("Duplicate SQLConfigEntry. " + entry.getKey() + " has been registered");
            }
            final Map<String, ConfigEntry<?>> updatedMap = new HashMap<>(plannerConfEntries);
            updatedMap.put(entry.getKey(), entry);
            plannerConfEntries = updatedMap;
        }
    }

    private static ConfigBuilder buildConf(String key) {
        return new ConfigBuilder(key).onCreate(PlannerConf::register);
    }

    private ConfigEntry<?> getConfigEntry(String key) {
        return plannerConfEntries.get(key);
    }

    private boolean containsConfigEntry(ConfigEntry<?> entry) {
        return getConfigEntry(entry.getKey()) == entry;
    }


}
