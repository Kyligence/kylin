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
package org.apache.kylin.common.persistence;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.util.Unsafe;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.log4j.Log4j;

@Getter
@Log4j
public class RawResourceFilter {
    final List<Condition> conditions;

    public RawResourceFilter() {
        this(new ArrayList<>());
    }

    public RawResourceFilter(List<Condition> conditions) {
        this.conditions = conditions;
    }

    public RawResourceFilter addConditions(Condition condition) {
        this.conditions.add(condition);
        return this;
    }

    public RawResourceFilter addConditions(String name, List<Object> values, Operator op) {
        this.conditions.add(new Condition(name, values, op));
        return this;
    }

    public boolean isMatch(RawResource rawResource) {
        return conditions.stream().allMatch(condition -> {
            Object value = rawResource.getMetaKey();
            switch (condition.name) {
            case "metaKey":
                break;
            case "uuid":
                value = rawResource.getUuid();
                break;
            case "project":
                value = rawResource.getProject();
                break;
            case "mvcc":
                value = rawResource.getMvcc();
                break;
            case "ts":
                value = rawResource.getTs();
                break;
            case "reservedFiled1":
                value = rawResource.getReservedFiled1();
                break;
            case "reservedFiled2":
                value = rawResource.getReservedFiled2();
                break;
            case "reservedFiled3":
                value = rawResource.getReservedFiled3();
                break;
            case "id":
            case "content":
                throw new IllegalArgumentException("id & content field can't be used as filter condition");
            default:
                try {
                    Field field = rawResource.getClass().getDeclaredField(condition.name);
                    Unsafe.changeAccessibleObject(field, true);
                    value = field.get(rawResource);
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    throw new IllegalArgumentException("Invalid condition:" + condition.name, e);
                }
            }
            if (value != null) {
                switch (condition.getOp()) {
                case EQUAL:
                    return Objects.equals(condition.values.get(0), value);
                case EQUAL_CASE_INSENSITIVE:
                    assert value instanceof String
                            : "[" + condition.name + "] '" + value + "' is not String, can't compare case insensitive";
                    return ((String) value).equalsIgnoreCase((String) condition.values.get(0));
                case LIKE_CASE_INSENSITIVE:
                    assert value instanceof String : "[" + condition.name + "] '" + value
                            + "' is not String, can't compare like case insensitive";
                    return StringUtils.containsIgnoreCase((String) value, (String) condition.values.get(0));
                case IN:
                    return condition.values.contains(value);
                case GT:
                    assert value instanceof Long;
                    return (Long) value > ((Long) condition.values.get(0));
                case LT:
                    assert value instanceof Long;
                    return (Long) value < ((Long) condition.values.get(0));
                case LE:
                    assert value instanceof Long;
                    return (Long) value <= ((Long) condition.values.get(0));
                case GE:
                    assert value instanceof Long;
                    return (Long) value >= ((Long) condition.values.get(0));
                default:
                    throw new UnsupportedOperationException("Operator not supported:" + condition.getOp());
                }
            }
            //Maybe rawResource is broken, just return false
            return false;
        });
    }

    public static RawResourceFilter equalFilter(String key, String value) {
        return simpleFilter(Operator.EQUAL, key, value);
    }

    public static RawResourceFilter simpleFilter(Operator op, String key, Object value) {
        return new RawResourceFilter().addConditions(key, Collections.singletonList(value), op);
    }

    @Getter
    @AllArgsConstructor
    public static class Condition {
        /**
         * java bean property name.
         * e.g. String modeName;
         */
        private final String name;
        private final List<Object> values;
        private final Operator op;

        /**
         * Used for FileSystem-filter eval expression.
         */
        @Setter
        private String eval;

        public Condition(String name, List<Object> values, Operator op) {
            this(name, values, op, null);
        }
    }

    public enum Operator {
        EQUAL, EQUAL_CASE_INSENSITIVE, IN, GT, LT, LIKE_CASE_INSENSITIVE, LE, GE
    }
}
