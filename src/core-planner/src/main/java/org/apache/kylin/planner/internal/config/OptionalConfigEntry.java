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
import java.util.Optional;

public class OptionalConfigEntry<T> extends ConfigEntry<Optional<T>> {
    private final ConfigHelper.Converter<String, T> rawValueConverter;
    public final ConfigHelper.Converter<T, String> rawStringConverter;

    protected OptionalConfigEntry(String key, String prependedKey, String prependSeparator, List<String> alternatives,
            ConfigHelper.Converter<String, T> rawValueConverter, ConfigHelper.Converter<T, String> rawStringConverter,
            String doc, boolean isPublic, String version) {
        super(key, prependedKey, prependSeparator, alternatives, s -> Optional.of(rawValueConverter.convert(s)),
                v -> v.map(rawStringConverter::convert).orElse(null), doc, isPublic, version);
        this.rawValueConverter = rawValueConverter;
        this.rawStringConverter = rawStringConverter;
    }

    @Override
    public String defaultValueString() {
        return UNDEFINED;
    }

    @Override
    public Optional<T> readFrom(ConfigReader reader) {
        return readString(reader).map(rawValueConverter::convert);
    }
}
