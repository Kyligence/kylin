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

package org.apache.kylin.tool.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CreateTableFromJsonTest {

    String path = "src/test/resources/table_cc_cleanup/metadata/AL_4144/table/";
    private final ByteArrayOutputStream outContent = new ByteArrayOutputStream();

    @BeforeEach
    void setUpStreams() {
        System.setOut(new PrintStream(outContent));
    }

    @AfterEach
    void cleanUpStreams() {
        System.setOut(null);
    }

    @Test
    void test() throws IOException {
        CreateTableFromJson.main(new String[] { path });
        String result = outContent.toString();
        String[] splits = result.split("\n");
        Assertions.assertEquals("create database `CAP`;", splits[0]);
        Assertions.assertEquals("use `CAP`;", splits[1]);
        Assertions.assertEquals("create table `ST`(`SID` varchar(4096), `CID` varchar(4096), `TID` varchar(4096), "
                + "`S_NAME` varchar(4096), `S_DATE` date, `DT` varchar(4096), `CC1` double);", splits[2]);
    }
}
