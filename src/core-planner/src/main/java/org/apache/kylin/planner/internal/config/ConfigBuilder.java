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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.kylin.common.util.ByteUnit;

public class ConfigBuilder {

    private final String key;
    private String prependedKey;
    private String prependSeparator = "";
    private boolean isPublic = true;
    private String doc = "";
    private String version = "";
    private Consumer<ConfigEntry<?>> onCreate;
    private final List<String> alternatives = new ArrayList<>();

    public ConfigBuilder(String key) {
        this.key = key;
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

    public boolean isPublic() {
        return isPublic;
    }

    public String getDoc() {
        return doc;
    }

    public String getVersion() {
        return version;
    }

    public Optional<Consumer<ConfigEntry<?>>> getOnCreate() {
        return Optional.ofNullable(onCreate);
    }

    public List<String> getAlternatives() {
        return alternatives;
    }

    public ConfigBuilder internal() {
        isPublic = false;
        return this;
    }

    public ConfigBuilder doc(String s) {
        doc = s;
        return this;
    }

    public ConfigBuilder version(String v) {
        version = v;
        return this;
    }

    public ConfigBuilder onCreate(Consumer<ConfigEntry<?>> callback) {
        onCreate = callback;
        return this;
    }

    public ConfigBuilder withPrepended(String key, String separator) {
        prependedKey = key;
        prependSeparator = separator != null ? separator : " ";
        return this;
    }

    public ConfigBuilder withAlternative(String key) {
        alternatives.add(key);
        return this;
    }

    public TypeConfigBuilder<Integer> intConf() {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, (String s) -> ConfigHelper.toNumber(s, Integer::parseInt, key, "int"));
    }

    public TypeConfigBuilder<Long> longConf() {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, (String s) -> ConfigHelper.toNumber(s, Long::parseLong, key, "long"));
    }

    public TypeConfigBuilder<Double> doubleConf() {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, (String s) -> ConfigHelper.toNumber(s, Double::parseDouble, key, "double"));
    }

    public TypeConfigBuilder<Boolean> booleanConf() {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, (String s) -> ConfigHelper.toBoolean(s, key));
    }

    public TypeConfigBuilder<String> stringConf() {
        return new TypeConfigBuilder<>(this, v -> v);
    }

    public TypeConfigBuilder<Long> timeConf(TimeUnit unit) {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, value -> ConfigHelper.timeFromString(value, unit),
                value -> ConfigHelper.timeToString(value, unit));
    }

    public TypeConfigBuilder<Long> bytesConf(ByteUnit unit) {
        checkPrependConfig();
        return new TypeConfigBuilder<>(this, value -> ConfigHelper.byteFromString(value, unit),
                value -> ConfigHelper.byteToString(value, unit));
    }

    public <T> ConfigEntry<T> fallbackConf(ConfigEntry<T> fallback) {
        FallbackConfigEntry<T> entry = new FallbackConfigEntry<>(key, prependedKey, prependSeparator, alternatives, doc,
                isPublic, version, fallback);
        if (onCreate != null) {
            onCreate.accept(entry);
        }
        return entry;
    }

    private void checkPrependConfig() {
        if (prependedKey != null) {
            throw new IllegalArgumentException(key + " type must be string if prepend used");
        }
    }
}
