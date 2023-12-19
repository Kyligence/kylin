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

package org.apache.kylin.planner.algorithm;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.planner.plans.CubePlanStats;

import lombok.extern.slf4j.Slf4j;

/**
 * Elena Baralis, Stefano Paraboschi, Ernest Teniente: Materialized Views Selection in a Multidimensional Database.
 * VLDB 1997: 156-165.
 * [Online]. Available: https://www.vldb.org/conf/1997/P156.PDF
 */
@Slf4j
public class ESE97Variant implements BenefitCalculator<BitSet> {

    private static final double QUERY_UNCERTAIN_RATIO = 0.2d;
    private static final double UPDATE_UNCERTAIN_RATIO = 0.2d;

    // IC refers to importance coefficient
    private static final double IC_QUERY = 0.7d;
    private static final double IC_UPDATE = 0.3d;

    private final TupleCountTree mandatoryTree;
    private final TupleCountTree existingTree;

    private final Map<BitSet, Double> queryProbMap;
    private final Map<BitSet, Double> updateProbMap;

    private final Function<BitSet, Long> countFunc;

    public ESE97Variant(CubePlanStats planStats) {
        // essential inputs
        final List<BitSet> cuboidList = planStats.cuboidList();
        final List<BitSet> mandatoryList = planStats.mandatoryList();
        final List<BitSet> existingList = planStats.existingList();

        // essential intermediate trees
        final long ceilingRowCount = planStats.ceilingRowCount();
        this.countFunc = c -> planStats.tupleCount(c, ceilingRowCount);
        this.mandatoryTree = treeOf(mandatoryList, planStats.ceilingBits(), ceilingRowCount, countFunc);
        this.existingTree = treeOf(existingList, mandatoryTree, countFunc);

        // essential intermediate stats
        this.queryProbMap = queryProbabilityMap(cuboidList, planStats::queryFrequency);
        final int ceilingUpdateFrequency = planStats.ceilingUpdateFrequency();
        this.updateProbMap = updateProbabilityMap(cuboidList,
                c -> planStats.updateFrequency(c, ceilingUpdateFrequency));
    }

    @Override
    public double benefitOf(List<BitSet> cuboidList) {
        final TupleCountTree tree = treeOf(cuboidList, mandatoryTree, countFunc);
        double queryCost = queryProbMap.entrySet().stream().map(e -> tree.findBestCost(e.getKey()) * e.getValue())
                .reduce(0d, Double::sum);
        double updateCost = updateProbMap.entrySet().stream()
                .map(e -> existingTree.findBestCost(e.getKey()) * e.getValue()).reduce(0d, Double::sum);
        return 1.0d / (1.0d + queryCost * IC_QUERY + updateCost * IC_UPDATE);
    }

    private TupleCountTree treeOf(List<BitSet> cuboidList, BitSet ceilingBits, long ceilingRowCount,
            Function<BitSet, Long> countFunc) {
        TupleCountTree tree = new TupleCountTree(ceilingBits, ceilingRowCount);
        tree.addAll(cuboidList.stream().collect(Collectors.toMap(Function.identity(), countFunc)));
        return tree;
    }

    private TupleCountTree treeOf(List<BitSet> cuboidList, TupleCountTree copyTree, Function<BitSet, Long> countFunc) {
        TupleCountTree tree = copyTree.copy();
        tree.addAll(cuboidList.stream().collect(Collectors.toMap(Function.identity(), countFunc)));
        return tree;
    }

    private Map<BitSet, Double> queryProbabilityMap(List<BitSet> cuboidList, Function<BitSet, Long> func) {
        if (cuboidList == null || cuboidList.isEmpty()) {
            return Collections.emptyMap();
        }
        final double unit = BigDecimal.valueOf(QUERY_UNCERTAIN_RATIO)
                .divide(new BigDecimal(cuboidList.size()), 15, RoundingMode.HALF_EVEN).doubleValue();
        final Map<BitSet, Long> freqMap = cuboidList.stream().collect(Collectors.toMap(Function.identity(), func));
        final long total = freqMap.values().stream().reduce(0L, Long::sum);
        if (total <= 0L) {
            return cuboidList.stream().collect(Collectors.toMap(Function.identity(), c -> unit));
        }
        final double ratio = 1.0d - QUERY_UNCERTAIN_RATIO;
        return cuboidList.stream().collect(
                ImmutableMap.toImmutableMap(Function.identity(), c -> (unit + ratio * freqMap.get(c) / total)));
    }

    private Map<BitSet, Double> updateProbabilityMap(List<BitSet> cuboidList, Function<BitSet, Integer> func) {
        if (cuboidList == null || cuboidList.isEmpty()) {
            return Collections.emptyMap();
        }
        final double unit = BigDecimal.valueOf(UPDATE_UNCERTAIN_RATIO)
                .divide(new BigDecimal(cuboidList.size()), 15, RoundingMode.HALF_EVEN).doubleValue();
        final Map<BitSet, Integer> freqMap = cuboidList.stream().collect(Collectors.toMap(Function.identity(), func));
        final int total = freqMap.values().stream().reduce(0, Integer::sum);
        if (total <= 0) {
            return cuboidList.stream().collect(Collectors.toMap(Function.identity(), c -> unit));
        }
        final double ratio = 1.0d - UPDATE_UNCERTAIN_RATIO;
        return cuboidList.stream().collect(
                ImmutableMap.toImmutableMap(Function.identity(), c -> (unit + ratio * freqMap.get(c) / total)));

    }

}
