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

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.CrossoverPolicy;
import org.apache.commons.math3.genetics.GeneticAlgorithm;
import org.apache.commons.math3.genetics.MutationPolicy;
import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.SelectionPolicy;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.apache.kylin.planner.algorithm.CuboidSelector;

public abstract class GeneticHeuristic implements CuboidSelector {

    /**
     * the init population size.
     */
    protected static final int POPULATION_SIZE = 500;

    /**
     * the max population size.
     */
    protected static final int MAX_POPULATION_SIZE = 550;

    protected static final int MAX_GENERATION_COUNT = 1100;

    /**
     * the rate of crossover for the algorithm.
     */
    private static final double CROSSOVER_RATE = 0.9d;

    /**
     * the rate of mutation for the algorithm.
     */
    private static final double MUTATION_RATE = 0.01d;

    private final GeneticAlgorithm algo;

    protected GeneticHeuristic() {
        this.algo = new GeneticAlgorithm(crossoverPolicy(), CROSSOVER_RATE, mutationPolicy(), MUTATION_RATE,
                selectionPolicy());
    }

    protected abstract Population population();

    protected abstract CrossoverPolicy crossoverPolicy();

    protected abstract MutationPolicy mutationPolicy();

    protected abstract SelectionPolicy selectionPolicy();

    protected abstract StoppingCondition stoppingCondition();

    protected final Chromosome findBest() {

        // initial population
        Population pop = population();

        // stop condition
        StoppingCondition cond = stoppingCondition();

        // evolution
        Population evo = algo.evolve(pop, cond);

        // fittest chromosome
        return evo.getFittestChromosome();
    }

}
