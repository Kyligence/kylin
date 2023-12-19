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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

public abstract class ConfigEntry<T> {

    protected static final String UNDEFINED = "<undefined>";
    private static final ConcurrentMap<String, Object> knownConfigs = new ConcurrentHashMap<>();

    private final String key;
    private final String prependedKey;
    private final String prependSeparator;
    private final List<String> alternatives;
    private final String doc;
    private final boolean isPublic;
    private final String version;

    protected final ConfigHelper.Converter<String, T> valueConverter;
    public final ConfigHelper.Converter<T, String> stringConverter;

    protected ConfigEntry(String key, String prependedKey, String prependSeparator, List<String> alternatives,
            ConfigHelper.Converter<String, T> valueConverter, ConfigHelper.Converter<T, String> stringConverter,
            String doc, boolean isPublic, String version) {
        this.key = key;
        this.prependedKey = prependedKey;
        this.prependSeparator = prependSeparator;
        this.alternatives = alternatives;
        this.valueConverter = valueConverter;
        this.stringConverter = stringConverter;
        this.doc = doc;
        this.isPublic = isPublic;
        this.version = version;
        registerEntry(this);
    }

    public String getKey() {
        return key;
    }

    public Optional<String> getPrependedKey() {
        return Optional.ofNullable(prependedKey);
    }

    public String getPrependSeparator() {
        return prependSeparator;
    }

    public List<String> getAlternatives() {
        return alternatives;
    }

    public String getDoc() {
        return doc;
    }

    public boolean isPublic() {
        return isPublic;
    }

    public String getVersion() {
        return version;
    }

    public T convertValue(String value) {
        return valueConverter.convert(value);
    }

    public abstract String defaultValueString();

    protected Optional<String> readString(ConfigReader reader) {
        Optional<String> result = Stream.concat(Stream.of(key), alternatives.stream()).map(reader::get)
                .reduce((val1, val2) -> val1.isPresent() ? val1 : val2).orElse(Optional.empty());
        if (prependedKey != null) {
            return valueWithPrepended(reader.get(prependedKey), result);
        }
        return result;
    }

    private Optional<String> valueWithPrepended(Optional<String> prepended, Optional<String> value) {
        if (prepended.isPresent()) {
            return value.map(s -> Optional.of(prepended.get() + prependSeparator + s)).orElse(prepended);
        }
        return value;
    }

    public abstract T readFrom(ConfigReader reader);

    public Optional<T> defaultValue() {
        return Optional.empty();
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "ConfigEntry(key=%s, defaultValue=%s, doc=%s, public=%b, version=%s)", key,
                defaultValueString(), doc, isPublic, version);
    }

    public static void registerEntry(ConfigEntry<?> entry) {
        Object existing = knownConfigs.putIfAbsent(entry.key, entry);
        if (existing != null) {
            throw new IllegalArgumentException("Config entry " + entry.key + " already registered!");
        }
    }

    public static Object findEntry(String key) {
        return knownConfigs.get(key);
    }
}
