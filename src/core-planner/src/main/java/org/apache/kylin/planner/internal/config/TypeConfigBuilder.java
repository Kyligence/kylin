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
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class TypeConfigBuilder<T> {
    private final ConfigBuilder parent;
    private final ConfigHelper.Converter<String, T> converter;
    private final ConfigHelper.Converter<T, String> stringConverter;

    public TypeConfigBuilder(ConfigBuilder parent, ConfigHelper.Converter<String, T> converter,
            ConfigHelper.Converter<T, String> stringConverter) {
        this.parent = parent;
        this.converter = converter;
        this.stringConverter = stringConverter;
    }

    public TypeConfigBuilder(ConfigBuilder parent, ConfigHelper.Converter<String, T> converter) {
        this(parent, converter, t -> Optional.ofNullable(t).map(Object::toString).orElse(null));
    }

    public TypeConfigBuilder<T> transform(ConfigHelper.Converter<T, T> fn) {
        return new TypeConfigBuilder<>(parent, s -> fn.convert(converter.convert(s)), stringConverter);
    }

    public TypeConfigBuilder<T> checkValue(ConfigHelper.Converter<T, Boolean> validator, String errorMsg) {
        return transform(v -> {
            if (!validator.convert(v)) {
                throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "'%s' in %s is invalid. %s", v, parent.getKey(), errorMsg));
            }
            return v;
        });
    }

    public TypeConfigBuilder<T> checkValues(Set<T> validValues) {
        return transform(v -> {
            if (!validValues.contains(v)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT,
                        "The value of %s should be one of %s, but was %s",
                        parent.getKey(), validValues, v));
            }
            return v;
        });
    }

    public TypeConfigBuilder<List<T>> toSequence() {
        return new TypeConfigBuilder<>(parent, s -> ConfigHelper.stringToList(s, converter),
                v -> ConfigHelper.listToString(v, stringConverter));
    }

    public OptionalConfigEntry<T> createOptional() {
        OptionalConfigEntry<T> entry = new OptionalConfigEntry<T>(parent.getKey(),
                parent.getPrependedKey().orElse(null), parent.getPrependSeparator(), parent.getAlternatives(),
                converter, stringConverter, parent.getDoc(), parent.isPublic(), parent.getVersion());
        parent.getOnCreate().ifPresent(callback -> callback.accept(entry));
        return entry;
    }

    public ConfigEntry<T> createWithDefault(T defaultValue) {
        T transformedDefault = converter.convert(stringConverter.convert(defaultValue));
        ConfigEntry<T> entry = new ConfigEntryWithDefault<T>(parent.getKey(), parent.getPrependedKey().orElse(null),
                parent.getPrependSeparator(), parent.getAlternatives(), transformedDefault, converter, stringConverter,
                parent.getDoc(), parent.isPublic(), parent.getVersion());
        parent.getOnCreate().ifPresent(callback -> callback.accept(entry));
        return entry;
    }

    public ConfigEntry<T> createWithDefaultFunction(Supplier<T> defaultFunc) {
        ConfigEntry<T> entry = new ConfigEntryWithDefaultFunction<T>(parent.getKey(),
                parent.getPrependedKey().orElse(null), parent.getPrependSeparator(), parent.getAlternatives(),
                defaultFunc, converter, stringConverter, parent.getDoc(), parent.isPublic(), parent.getVersion());
        parent.getOnCreate().ifPresent(callback -> callback.accept(entry));
        return entry;
    }

    public ConfigEntry<T> createWithDefaultString(String defaultValue) {
        ConfigEntry<T> entry = new ConfigEntryWithDefaultString<T>(parent.getKey(),
                parent.getPrependedKey().orElse(null), parent.getPrependSeparator(), parent.getAlternatives(),
                defaultValue, converter, stringConverter, parent.getDoc(), parent.isPublic(), parent.getVersion());
        parent.getOnCreate().ifPresent(callback -> callback.accept(entry));
        return entry;
    }
}
