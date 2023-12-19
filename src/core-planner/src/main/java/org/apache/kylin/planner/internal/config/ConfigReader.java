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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.SerializationUtils;

import scala.util.matching.Regex;

public class ConfigReader {

    private static final Pattern REF_RE = Pattern.compile("\\$\\{(?:(\\w+?):)?(\\S+?)\\}");

    private final Map<String, ConfigProvider> bindings = new HashMap<>();
    private final ConfigProvider conf;
    private final InheritableThreadLocal<Map<String, String>> localProperties = //
            new InheritableThreadLocal<Map<String, String>>() {
        @Override
        protected Map<String, String> childValue(Map<String, String> parent) {
            return SerializationUtils.clone((HashMap<String, String>) parent);
        }

        @Override
        protected Map<String, String> initialValue() {
            return new HashMap<>();
        }
    };

    public ConfigReader(ConfigProvider conf) {
        this.conf = conf;
        bind(null, conf);
        bindEnv(new EnvProvider());
        bindSystem(new SystemProvider());
    }

    public ConfigReader(Map<String, String> conf) {
        this(new MapProvider(conf));
    }

    /**
     * Binds a prefix to a provider. This method is not thread-safe and should be called
     * before the instance is used to expand values.
     */
    public ConfigReader bind(String prefix, ConfigProvider provider) {
        bindings.put(prefix, provider);
        return this;
    }

    public ConfigReader bind(String prefix, Map<String, String> values) {
        return bind(prefix, new MapProvider(values));
    }

    public ConfigReader bindEnv(ConfigProvider provider) {
        return bind("env", provider);
    }

    public ConfigReader bindSystem(ConfigProvider provider) {
        return bind("system", provider);
    }

    /**
     * Reads a configuration key from the default provider, and apply variable substitution.
     */
    public Optional<String> get(String key) {
        return Optional
                .ofNullable(Optional.ofNullable(localProperties.get().get(key)).orElse(conf.get(key).orElse(null)))
                .map(this::substitute);
    }

    /**
     * Perform variable substitution on the given input string.
     */
    public String substitute(String input) {
        return substitute(input, new HashSet<>());
    }

    private String substitute(String input, Set<String> usedRefs) {
        if (input == null) {
            return null;
        }

        Matcher matcher = REF_RE.matcher(input);
        int lastIndex = 0;
        StringBuilder output = new StringBuilder();
        while (matcher.find()) {
            final String prefix = matcher.group(1);
            final String name = matcher.group(2);
            final String ref = (prefix == null) ? name : prefix + ":" + name;

            if (usedRefs.contains(ref)) {
                throw new IllegalArgumentException("Circular reference in " + input + ": " + ref);
            }

            String replacement = Optional.ofNullable(bindings.get(prefix)).flatMap(p -> getOrDefault(p, name))
                    .map(v -> {
                        HashSet<String> set = new HashSet<>(usedRefs);
                        set.add(ref);
                        return substitute(v, set);
                    }).orElse(matcher.group());
            output.append(input, lastIndex, matcher.start());
            output.append(Regex.quoteReplacement(replacement));
            lastIndex = matcher.end();
        }
        output.append(input, lastIndex, input.length());

        return output.toString();
    }

    /**
     * Gets the value of a config from the given `ConfigProvider`. If no value is found for this
     * config, and the `ConfigEntry` defines this config has default value, return the default value.
     */
    private Optional<String> getOrDefault(ConfigProvider conf, String key) {
        return Optional.ofNullable(conf.get(key).orElseGet(() -> {
            Object entry = ConfigEntry.findEntry(key);
            if (entry instanceof ConfigEntryWithDefault<?>) {
                return ((ConfigEntryWithDefault<?>) entry).defaultValueString();
            } else if (entry instanceof ConfigEntryWithDefaultString<?>) {
                return ((ConfigEntryWithDefaultString<?>) entry).defaultValueString();
            } else if (entry instanceof ConfigEntryWithDefaultFunction<?>) {
                return ((ConfigEntryWithDefaultFunction<?>) entry).defaultValueString();
            } else if (entry instanceof FallbackConfigEntry<?>) {
                return getOrDefault(conf, ((FallbackConfigEntry<?>) entry).getFallback().getKey()).orElse(null);
            } else {
                return null;
            }
        }));
    }

}
