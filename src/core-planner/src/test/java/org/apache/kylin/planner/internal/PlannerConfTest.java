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

package org.apache.kylin.planner.internal;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kylin.common.util.ByteUnit;
import org.apache.kylin.planner.internal.config.ConfigBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.val;

public class PlannerConfTest {
    private static final String PREFIX = "kylin.planner.";
    private static PlannerConf conf;

    private String testKey(String name){
        return PREFIX + name;
    }

    @BeforeAll
    static void setUp() {
        conf = new PlannerConf();
    }

    @Test
    void testIntConf() {
        val iConf = new ConfigBuilder(testKey("int")).intConf().createWithDefault(1);
        PlannerConf.register(iConf);
        Assertions.assertEquals(1, (int) conf.getConf(iConf));
        conf.set(iConf, 2);
        Assertions.assertEquals(2, (int) conf.getConf(iConf));
    }

    @Test
    void testLongConf() {
        val lConf = new ConfigBuilder(testKey("long")).longConf().createWithDefault(0L);
        PlannerConf.register(lConf);
        conf.set(lConf, 1234L);
        Assertions.assertEquals(1234L, (long) conf.getConf(lConf));
    }

    @Test
    void testDoubleConf() {
        val dConf = new ConfigBuilder(testKey("double")).doubleConf().createWithDefault(0.0);
        PlannerConf.register(dConf);
        conf.set(dConf, 20.0);
        Assertions.assertEquals(20.0, conf.getConf(dConf));
    }

    @Test
    void testBooleanConf() {
        val bConf = new ConfigBuilder(testKey("boolean")).booleanConf().createWithDefault(false);
        PlannerConf.register(bConf);
        Assertions.assertFalse(conf.getConf(bConf));
        conf.set(bConf, true);
        Assertions.assertTrue(conf.getConf(bConf));
    }

    @Test
    void testOptionalConf() {
        val optionalConf = new ConfigBuilder(testKey("optional")).intConf().createOptional();
        PlannerConf.register(optionalConf);
        Assertions.assertFalse(conf.getConf(optionalConf).isPresent());
        conf.set(optionalConf, 1);
        Assertions.assertEquals(conf.getConf(optionalConf), Optional.of(1));
    }

    @Test
    void testFallbackConf() {
        val parentConf = new ConfigBuilder(testKey("parent1")).intConf().createWithDefault(1);
        val confWithFallback = new ConfigBuilder(testKey("fallback1")).fallbackConf(parentConf);
        PlannerConf.register(parentConf);
        PlannerConf.register(confWithFallback);
        Assertions.assertEquals(1, (int) conf.getConf(confWithFallback));
        conf.set(confWithFallback, 2);
        Assertions.assertEquals(1, (int) conf.getConf(parentConf));
        Assertions.assertEquals(2, (int) conf.getConf(confWithFallback));
    }

    @Test
    void testTimeConf() {
        val time = new ConfigBuilder(testKey("time")).timeConf(TimeUnit.SECONDS)
                .createWithDefaultString("1h");
        PlannerConf.register(time);
        Assertions.assertEquals(3600L, (long) conf.getConf(time));
        conf.setConfString(time.getKey(), "1m");
        Assertions.assertEquals(60L, (long) conf.getConf(time));
    }

    @Test
    void testBytesConf() {
        val bytes = new ConfigBuilder(testKey("bytes")).bytesConf(ByteUnit.KiB)
                .createWithDefaultString("1m");
        PlannerConf.register(bytes);
        Assertions.assertEquals(1024L, (long) conf.getConf(bytes));
        conf.setConfString(bytes.getKey(), "1k");
        Assertions.assertEquals(1L, (long) conf.getConf(bytes));
        conf.setConfString(bytes.getKey(), "2048");
        Assertions.assertEquals(2048, (long) conf.getConf(bytes));
    }

    @Test
    void testStringConf() {
        val seq = new ConfigBuilder(testKey("seq")).stringConf().toSequence().createWithDefault(new ArrayList<>());
        PlannerConf.register(seq);
        conf.setConfString(seq.getKey(), "1,,2, 3 , , 4");
        Assertions.assertArrayEquals(conf.getConf(seq).toArray(), Arrays.asList("1", "2", "3", "4").toArray());
        conf.set(seq, Arrays.asList("1", "2"));
        Assertions.assertArrayEquals(conf.getConf(seq).toArray(), Arrays.asList("1", "2").toArray());
    }

    @Test
    void testIntSeqConf() {
        val seq = new ConfigBuilder(testKey("intSeq")).intConf().toSequence().createWithDefault(new ArrayList<>());
        PlannerConf.register(seq);
        conf.setConfString(seq.getKey(), "1,,2, 3 , , 4");
        Assertions.assertArrayEquals(conf.getConf(seq).toArray(), Arrays.asList(1, 2, 3, 4).toArray());
        conf.set(seq, Arrays.asList(1, 2));
        Assertions.assertArrayEquals(conf.getConf(seq).toArray(), Arrays.asList(1, 2).toArray());
    }

    @Test
    void testTransformConf() {
        val transformationConf = new ConfigBuilder(testKey("transformation"))
                .stringConf()
                .transform(x -> x.toLowerCase(Locale.ROOT))
                .createWithDefault("FOO");
        PlannerConf.register(transformationConf);

        Assertions.assertEquals("foo", conf.getConf(transformationConf));
        conf.set(transformationConf, "BAR");
        Assertions.assertEquals("bar", conf.getConf(transformationConf));
    }

    @Test
    void testCheckValues() {
        val entry = new ConfigBuilder(testKey("checkValue")).intConf()
                .checkValue(value -> value >= 0, "value must be non-negative").createWithDefault(10);
        PlannerConf.register(entry);

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> conf.set(entry, -1));
        Assertions.assertEquals("'-1' in " + entry.getKey() + " is invalid. value must be non-negative",
                exception.getMessage());

        exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> new ConfigBuilder(testKey("checkValue")).intConf()
                        .checkValue(value -> value >= 0, "value must be non-negative").createWithDefault(-1));
        Assertions.assertEquals("'-1' in " + entry.getKey() + " is invalid. value must be non-negative",
                exception.getMessage());
    }

    @Test
    void testCheckValuesConf2() {
        val entry = new ConfigBuilder(testKey("enum"))
                .stringConf()
                .checkValues(new HashSet<>(Arrays.asList("a", "b", "c")))
                .createWithDefault("a");
        PlannerConf.register(entry);

        Assertions.assertEquals("a", conf.getConf(entry));

        conf.set(entry, "b");
        Assertions.assertEquals("b", conf.getConf(entry));

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class, () -> conf.set(entry, "d"));
        Assertions.assertEquals("The value of "+entry.getKey()+" should be one of [a, b, c], but was d",
                exception.getMessage());
    }

    @Test
    void testConversionError() {
        val conversionTest = new ConfigBuilder(testKey("conversionTest")).doubleConf().createOptional();
        PlannerConf.register(conversionTest);

        Exception exception = Assertions.assertThrows(IllegalArgumentException.class,
                () -> conf.setConfString(conversionTest.getKey(), "abc"));
        Assertions.assertEquals(conversionTest.getKey() + " should be double, but was abc", exception.getMessage());
    }

    @Test
    void testNullValue() {
        val stringConf = new ConfigBuilder(testKey("string")).stringConf().createWithDefault(null);
        PlannerConf.register(stringConf);
        Assertions.assertNull(conf.getConf(stringConf));
    }

    @Test
    void testDefaultFunction() {
        AtomicInteger value = new AtomicInteger(0);
        val iConf = new ConfigBuilder(testKey("intval")).intConf().createWithDefaultFunction(value::get);
        PlannerConf.register(iConf);
        Assertions.assertEquals(0, (int) conf.getConf(iConf));
        value.set(1);
        Assertions.assertEquals(1, (int) conf.getConf(iConf));
    }

    @Test
    void testAlternativeKey() {
        val iConf = new ConfigBuilder(testKey("a"))
                .withAlternative(testKey("b"))
                .withAlternative(testKey("c"))
                .intConf().createWithDefault(0);
        PlannerConf.register(iConf);

        // no key is set, return default value.
        Assertions.assertEquals(0, (int) conf.getConf(iConf));

        // the primary key is set, the alternative keys are not set, return the value of primary key.
        conf.setConfString(testKey("a"), "1");
        Assertions.assertEquals(1, (int) conf.getConf(iConf));

        // the primary key and alternative keys are all set, return the value of primary key.
        conf.setConfString(testKey("b"), "2");
        conf.setConfString(testKey("c"), "3");
        Assertions.assertEquals(1, (int) conf.getConf(iConf));

        // the primary key is not set, (some of) the alternative keys are set, return the value of the
        // first alternative key that is set.
        conf.remove(testKey("a"));
        Assertions.assertEquals(2, (int) conf.getConf(iConf));
        conf.remove(testKey("b"));
        Assertions.assertEquals(3, (int) conf.getConf(iConf));
    }

    @Test
    void testPrepend() {
        val prependedKey = testKey("prepended1");
        val prependedConf = new ConfigBuilder(prependedKey).stringConf().createOptional();
        val derivedConf = new ConfigBuilder(testKey("prepend1")).withPrepended(prependedKey, null).stringConf()
                .createOptional();
        PlannerConf.register(prependedConf);
        PlannerConf.register(derivedConf);

        conf.set(derivedConf, "1");
        Assertions.assertEquals("1", conf.getConf(derivedConf).orElse(""));

        conf.set(prependedConf, "2");
        Assertions.assertEquals("2 1", conf.getConf(derivedConf).orElse(""));
    }

    @Test
    void testPrepend2() {
        val prependedKey = testKey("prepended2");
        val prependedConf = new ConfigBuilder(prependedKey).stringConf().createOptional();
        val derivedConf = new ConfigBuilder(testKey("prepend2"))
                .withPrepended(prependedKey, ",")
                .stringConf()
                .createOptional();
        PlannerConf.register(prependedConf);
        PlannerConf.register(derivedConf);

        conf.set(derivedConf, "1");
        Assertions.assertEquals("1", conf.getConf(derivedConf).orElse(""));

        conf.set(prependedConf, "2");
        Assertions.assertEquals("2,1", conf.getConf(derivedConf).orElse(""));
    }

    @Test
    void testPrepend3() {
        val prependedKey = testKey("prepended3");
        val prependedConf = new ConfigBuilder(prependedKey).stringConf().createOptional();
        val derivedConf = new ConfigBuilder(testKey("prepend3"))
                .withPrepended(prependedKey, null)
                .stringConf()
                .createOptional();
        val confWithFallback = new ConfigBuilder(testKey("fallback2")).fallbackConf(derivedConf);
        PlannerConf.register(prependedConf);
        PlannerConf.register(derivedConf);
        PlannerConf.register(confWithFallback);

        Assertions.assertFalse(conf.getConf(confWithFallback).isPresent());

        conf.set(derivedConf, "1");
        Assertions.assertEquals("1", conf.getConf(confWithFallback).orElse(""));

        conf.set(prependedConf, "2");
        Assertions.assertEquals("2 1", conf.getConf(confWithFallback).orElse(""));

        conf.set(confWithFallback, Optional.of("3"));
        Assertions.assertEquals("3", conf.getConf(confWithFallback).orElse(""));
    }

    @Test
    void testPrependedFail() {
        String prependKey = "prependedFail";
        String prependedKey = "prependFail";
        Exception exception;
        
        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null).intConf());
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));

        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null).longConf());
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));

        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null).doubleConf());
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));

        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null).booleanConf());
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));

        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null)
                        .timeConf(TimeUnit.MILLISECONDS));
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));

        exception = Assertions.assertThrows(IllegalArgumentException.class, //
                () -> new ConfigBuilder(testKey(prependKey)).withPrepended(prependedKey, null)
                        .bytesConf(ByteUnit.BYTE));
        Assertions.assertTrue(exception.getMessage().contains("type must be string if prepend used"));
    }

    @Test
    void testOnCreate() {
        AtomicBoolean onCreateCalled = new AtomicBoolean(false);
        new ConfigBuilder(testKey("oc1")).onCreate(entry -> onCreateCalled.set(true)).intConf().createWithDefault(1);
        Assertions.assertTrue(onCreateCalled.get());

        onCreateCalled.set(false);
        new ConfigBuilder(testKey("oc2")).onCreate(entry -> onCreateCalled.set(true)).intConf().createOptional();
        Assertions.assertTrue(onCreateCalled.get());

        onCreateCalled.set(false);
        new ConfigBuilder(testKey("oc3")).onCreate(entry -> onCreateCalled.set(true)).intConf()
                .createWithDefaultString("1.0");
        Assertions.assertTrue(onCreateCalled.get());

        val fallback = new ConfigBuilder(testKey("oc4")).intConf().createWithDefault(1);
        onCreateCalled.set(false);
        new ConfigBuilder(testKey("oc5")).onCreate(entry -> onCreateCalled.set(true)).fallbackConf(fallback);
        Assertions.assertTrue(onCreateCalled.get());
    }
}
