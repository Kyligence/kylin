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

package org.apache.kylin.common.util;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

public class ClassUtilTest {

    @Test
    public void testFindContainingJar() throws ClassNotFoundException {
        Assert.assertTrue(ClassUtil.findContainingJar(Class.forName("org.apache.commons.logging.LogFactory"))
                .contains("commons-logging"));
        Assert.assertTrue(ClassUtil.findContainingJar(Class.forName("org.apache.commons.logging.LogFactory"), "slf4j")
                .contains("jcl-over-slf4j"));
    }

    @Test
    void testAddToClasspath() throws Exception {
        // Prepare tmp dir & jars
        File extraJarsDir = null;
        try {
            String tmpDir = System.getProperty("java.io.tmpdir");
            extraJarsDir = new File(tmpDir, "class_util_test_jars");
            extraJarsDir.mkdir();

            List<File> jarFiles = Arrays.asList(new File(extraJarsDir, "hive-common-3.3.1.jar"),
                    new File(extraJarsDir, "aws-java-sdk-core-1.11.901.jar"),
                    new File(extraJarsDir, "hadoop-aws-3.3.1.jar"),
                    new File(extraJarsDir, "hadoop-aws-3.3.1"),
                    new File(extraJarsDir, "no-match-file.jar"));
            for (File f : jarFiles) {
                f.createNewFile();
            }
            File singleJarFile = new File(extraJarsDir, "a-single-file.jar");
            singleJarFile.createNewFile();

            // Load jars
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            ClassUtil.addToClasspath(String.format("%s/*hive*", extraJarsDir.getCanonicalPath()), loader);
            ClassUtil.addToClasspath(String.format("%s/*aws*", extraJarsDir.getCanonicalPath()), loader);
            try {
                ClassUtil.addToClasspath(String.format("%s/*hive", extraJarsDir.getCanonicalPath()), loader);
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof IllegalArgumentException);
            }
            try {
                ClassUtil.addToClasspath(String.format("%s/hive*", extraJarsDir.getCanonicalPath()), loader);
            } catch (Exception e) {
                Assertions.assertTrue(e instanceof IllegalArgumentException);
            }
            ClassUtil.addToClasspath(singleJarFile.getCanonicalPath(), loader);

            URL[] urls = (URL[]) ReflectionTestUtils.invokeMethod(loader, "getURLs");
            List<URL> urlList = Stream.of(urls).filter(url -> {
                for (File f : jarFiles) {
                    if (url.getPath().contains(f.getName()) || url.getPath().contains(singleJarFile.getName())) {
                        return true;
                    }
                }
                return false;
            }).collect(Collectors.toList());
            Assertions.assertEquals(4, urlList.size());
        } finally {
            if (extraJarsDir != null) {
                FileUtils.forceDeleteOnExit(extraJarsDir);
            }
        }
    }

}
