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

package org.apache.kylin.planner.internal.config;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.util.ByteUnit;
import org.apache.kylin.common.util.SizeConvertUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.apache.kylin.planner.util.Utils;

public class ConfigHelper {

    public static <T> T toNumber(String s, Converter<String, T> converter, String key, String configType) {
        try {
            return converter.convert(s.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s should be %s, but was %s", key, configType, s));
        }
    }

    public static boolean toBoolean(String s, String key) {
        try {
            return Boolean.parseBoolean(s.trim());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "%s should be boolean, but was %s", key, s));
        }
    }

    public static <T> List<T> stringToList(String str, Converter<String, T> converter) {
        return Utils.stringToList(str).stream().map(converter::convert).collect(Collectors.toList());
    }

    public static <T> String listToString(List<T> v, Converter<T, String> stringConverter) {
        return v.stream().map(stringConverter::convert).collect(Collectors.joining(","));
    }

    public static long timeFromString(String str, TimeUnit unit) {
        return TimeUtil.timeStringAs(str, unit);
    }

    public static String timeToString(long v, TimeUnit unit) {
        return TimeUnit.MILLISECONDS.convert(v, unit) + "ms";
    }

    public static long byteFromString(String str, ByteUnit unit) {
        int multiplier = str.startsWith("-") ? -1 : 1;
        String input = str.startsWith("-") ? str.substring(1) : str;
        return multiplier * SizeConvertUtil.byteStringAs(input, unit);
    }

    public static String byteToString(long v, ByteUnit unit) {
        return unit.convertTo(v, ByteUnit.BYTE) + "b";
    }

    @FunctionalInterface
    public interface Converter<F, T> {
        T convert(F from);
    }
}
