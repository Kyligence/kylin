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
package org.apache.kylin.common.util;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.util.FileUtil;

public final class SystemInfoCollector {

    private static final long KIBI = 1L << 10;
    private static final long MEBI = 1L << 20;
    private static final long GIBI = 1L << 30;
    private static final long TEBI = 1L << 40;
    private static final long PEBI = 1L << 50;
    private static final long EXBI = 1L << 60;
    public static final String CONTAINER_MEMORY_USAGE_IN_BYTES = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
    public static final String CONTAINER_MEMORY_LIMIT_IN_BYTES = "/sys/fs/cgroup/memory/memory.limit_in_bytes";

    private static HardwareAbstractionLayer hal = null;

    static {
        init();
    }

    private static void init() {
        SystemInfo si = new SystemInfo();
        hal = si.getHardware();
    }

    public static Integer getAvailableMemoryInfo() {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return 6192;
        }
        long memAvailable = 0L;
        if (KylinConfig.getInstanceFromEnv().getMicroServiceMode() == null) {
            memAvailable = hal.getMemory().getAvailable();
        } else {
            long containerMemoryLimit = FileUtil.getLongFromFile(CONTAINER_MEMORY_LIMIT_IN_BYTES);
            long containerMemoryUsage = FileUtil.getLongFromFile(CONTAINER_MEMORY_USAGE_IN_BYTES);
            if (containerMemoryLimit == 0 || containerMemoryUsage == 0) {
                memAvailable = hal.getMemory().getAvailable();
            } else {
                memAvailable = containerMemoryLimit - containerMemoryUsage;
            }
        }

        return memAvailable % MEBI == 0
                ? Integer.parseInt(String.format(Locale.ROOT, "%d", memAvailable / MEBI))
                : Integer.parseInt(String.format(Locale.ROOT, "%.0f", (double) memAvailable / MEBI));
    }
}
