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

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.planner.util.BitUtil;

public class PlanStats {

    // cuboid -> query frequency
    private Map<BitSet, Long> queryFrequencyMap;

    // cuboid -> update frequency
    private Map<BitSet, Integer> updateFrequencyMap;

    // cuboid -> tuple count
    private Map<BitSet, Long> tupleCountMap;

    public PlanStats() {
        this.queryFrequencyMap = Maps.newHashMap();
        this.updateFrequencyMap = Maps.newHashMap();
        this.tupleCountMap = Maps.newHashMap();
    }

    public void mergeQueryFrequency(Map<BitSet, Long> freqMap) {
        this.queryFrequencyMap = Stream.of(queryFrequencyMap, freqMap).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, this::harmonicMean));
    }

    public void mergeTupleCount(Map<BitSet, Long> countMap) {
        this.tupleCountMap = Stream.of(tupleCountMap, countMap).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, this::harmonicMean));
    }

    public void mergeUpdateFrequency(Map<BitSet, Integer> freqMap) {
        this.updateFrequencyMap = Stream.of(updateFrequencyMap, freqMap).flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, this::harmonicMean));
    }

    public Long getTupleCount(BitSet cuboid) {
        return tupleCountMap.get(cuboid);
    }

    public Long getQueryFrequency(BitSet cuboid) {
        return queryFrequencyMap.get(cuboid);
    }

    public Integer getUpdateFrequency(BitSet cuboid) {
        return updateFrequencyMap.get(cuboid);
    }

    public List<BitSet> queryList() {
        return queryFrequencyMap.keySet().stream().map(BitUtil::cloneBits).collect(ImmutableList.toImmutableList());
    }

    private int harmonicMean(int x, int y) {
        if (x <= 0) {
            return y;
        }

        if (y <= 0) {
            return x;
        }
        return (int) Math.ceil(2.0d / (1.0d / x + 1.0d / y));
    }

    private long harmonicMean(long x, long y) {
        if (x <= 0L) {
            return y;
        }

        if (y <= 0L) {
            return x;
        }
        return (long) Math.ceil(2.0d / (1.0d / x + 1.0d / y));
    }
}
