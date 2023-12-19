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

class FallbackConfigEntry<T> extends ConfigEntry<T> {

    private final ConfigEntry<T> fallback;

    protected FallbackConfigEntry(String key, String prependedKey, String prependSeparator, List<String> alternatives,
            String doc, boolean isPublic, String version, ConfigEntry<T> fallback) {
        super(key, prependedKey, prependSeparator, alternatives, fallback.valueConverter, fallback.stringConverter, doc,
                isPublic, version);
        this.fallback = fallback;
    }

    public ConfigEntry<T> getFallback() {
        return fallback;
    }

    @Override
    public String defaultValueString() {
        return "<value of " + fallback.getKey() + ">";
    }

    @Override
    public T readFrom(ConfigReader reader) {
        return readString(reader).map(valueConverter::convert).orElseGet(() -> fallback.readFrom(reader));
    }
}
