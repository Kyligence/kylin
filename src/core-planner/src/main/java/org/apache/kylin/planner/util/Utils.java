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

package org.apache.kylin.planner.util;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Utils {

    private Utils() {
    }

    public static List<String> stringToList(String str) {
        return Arrays.stream(str.split(",")).map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

    public static Set<String> recentOneMonthDays() {
        final LocalDate today = LocalDate.now(ZoneId.systemDefault());
        final LocalDate oneMonthAgo = today.minusMonths(1);
        return Stream.iterate(today, d -> d.minusDays(1)).limit(ChronoUnit.DAYS.between(oneMonthAgo, today))
                // format: yyyy-MM-dd
                .map(d -> d.format(DateTimeFormatter.ISO_DATE)).collect(Collectors.toSet());
    }
}
