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

package org.apache.kylin.planner.algorithm.genetic;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.CrossoverPolicy;
import org.apache.commons.math3.genetics.ElitisticListPopulation;
import org.apache.commons.math3.genetics.FixedElapsedTime;
import org.apache.commons.math3.genetics.FixedGenerationCount;
import org.apache.commons.math3.genetics.MutationPolicy;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.SelectionPolicy;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.planner.algorithm.BenefitCalculator;
import org.apache.kylin.planner.algorithm.ESE97Variant;
import org.apache.kylin.planner.plans.CubePlanStats;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CountConstraintAlgorithm extends GeneticHeuristic {

    private final int countLimit;
    private final List<BitSet> mandatoryList;
    private final FixedElapsedTime timeoutCond;

    private final BitSpindle spindle;
    private final BenefitCalculator<BitSet> calculator;

    public CountConstraintAlgorithm(int countLimit, int changeLimit, CubePlanStats planStats) {
        this(countLimit, changeLimit, planStats, 30, TimeUnit.MINUTES);
    }

    public CountConstraintAlgorithm(int countLimit, int changeLimit, CubePlanStats planStats, long timeout,
            TimeUnit timeunit) {
        this.countLimit = countLimit;
        this.mandatoryList = planStats.mandatoryList();
        this.timeoutCond = new FixedElapsedTime(timeout, timeunit);

        this.spindle = BitSpindle.builder().maxSelectNum(countLimit).maxChangeNum(changeLimit)
                .mandatoryList(mandatoryList).existingList(planStats.existingList()).queryList(planStats.queryList())
                .cuboidList(planStats.cuboidList()).build();
        this.calculator = new ESE97Variant(planStats);
    }

    @Override
    public List<BitSet> select() {
        // wip: count limit shrank
        int mandatoryCount = mandatoryList.size();
        if (countLimit <= mandatoryCount) {
            log.info("Mandatory cuboid count reached count limit: {}/{}", mandatoryCount, countLimit);
            return mandatoryList;
        }
        // fittest chromosome
        BitChromosome chr = (BitChromosome) findBest();
        // cuboid list
        return spindle.getCuboidList(chr.getBits());
    }

    @Override
    protected Population population() {
        List<Chromosome> chrList = IntStream.range(0, POPULATION_SIZE).mapToObj(i -> {
            BitSet bits = spindle.randomEliteBits();
            return new BitChromosome(bits, spindle, calculator);
        }).collect(ImmutableList.toImmutableList());
        return new ElitisticListPopulation(chrList, MAX_POPULATION_SIZE, 0.8d);
    }

    @Override
    protected CrossoverPolicy crossoverPolicy() {
        return new BitCrossover();
    }

    @Override
    protected MutationPolicy mutationPolicy() {
        return new BitMutation();
    }

    @Override
    protected SelectionPolicy selectionPolicy() {
        return new RouletteWheel();
    }

    @Override
    protected StoppingCondition stoppingCondition() {
        // 1. max iteration count
        // 2. max time elapse
        return new MultipleCondition(new FixedGenerationCount(MAX_GENERATION_COUNT), timeoutCond);
    }

}
