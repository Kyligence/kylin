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

import java.util.List;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.ChromosomePair;
import org.apache.commons.math3.genetics.GeneticAlgorithm;
import org.apache.commons.math3.genetics.ListPopulation;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.SelectionPolicy;
import org.apache.kylin.guava30.shaded.common.collect.Lists;

public class RouletteWheel implements SelectionPolicy {

    @Override
    public ChromosomePair select(Population pop) throws IllegalArgumentException {

        if (pop instanceof ListPopulation) {
            List<Chromosome> chrList = Lists.newArrayList(((ListPopulation) pop).getChromosomes());
            double totalFitness = chrList.stream().map(Chromosome::getFitness).reduce(0d, Double::sum);

            return new ChromosomePair(rouletteWheel(chrList, totalFitness), rouletteWheel(chrList, totalFitness));

        }

        throw new IllegalArgumentException("roulette wheel selection only works on ListPopulation");
    }

    private Chromosome rouletteWheel(List<Chromosome> chrList, double totalFitness) {
        // wip: roulette wheel theory explanation
        double score = 0d;
        double rand = GeneticAlgorithm.getRandomGenerator().nextDouble() * totalFitness;
        for (Chromosome chr : chrList) {
            double edge = score + chr.getFitness();
            if (rand >= score && rand < edge) {
                return chr;
            }
            score = edge;
        }

        return null;
    }
}
