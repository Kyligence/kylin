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

package org.apache.kylin.planner.stats;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.NDataModel;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@NoArgsConstructor
@Getter
@Setter
@Slf4j
public class RawRecInfo {

    private String recId;
    private long recTimes;
    private long totalCpuTime;
    private RecInfo recInfo;

    public static RawRecInfo parse(String input) {
        try {
            return JsonUtil.readValue(input, RawRecInfo.class);
        } catch (IOException e) {
            log.warn("Cannot parse content {}", input, e);
            return null;
        }
    }

    @NoArgsConstructor
    @Getter
    @Setter
    public static class RecInfo {
        private List<String> columns;
        private List<String> dimensions;
        private List<NDataModel.Measure> measures;
        private List<String> shardBy;
        private List<String> sortBy;
        private Map<String, String> ccExpression;
        private Map<String, String> ccType;
    }

}
