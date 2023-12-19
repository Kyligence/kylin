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

class ConfigEntryWithDefaultString<T> extends ConfigEntry<T> {
    private final String defaultValue;

    protected ConfigEntryWithDefaultString(String key, String prependedKey, String prependSeparator,
            List<String> alternatives, String defaultValue, ConfigHelper.Converter<String, T> valueConverter,
            ConfigHelper.Converter<T, String> stringConverter, String doc, boolean isPublic, String version) {
        super(key, prependedKey, prependSeparator, alternatives, valueConverter, stringConverter, doc, isPublic,
                version);
        this.defaultValue = defaultValue;
    }

    @Override
    public Optional<T> defaultValue() {
        return Optional.of(valueConverter.convert(defaultValue));
    }

    @Override
    public String defaultValueString() {
        return defaultValue;
    }

    @Override
    public T readFrom(ConfigReader reader) {
        return valueConverter.convert(readString(reader).orElse(reader.substitute(defaultValue)));
    }
}
