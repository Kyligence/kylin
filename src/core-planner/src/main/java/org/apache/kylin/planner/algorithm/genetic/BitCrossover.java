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

import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.exception.util.DummyLocalizable;
import org.apache.commons.math3.genetics.Chromosome;
import org.apache.commons.math3.genetics.ChromosomePair;
import org.apache.commons.math3.genetics.CrossoverPolicy;
import org.apache.kylin.planner.algorithm.BenefitCalculator;

public class BitCrossover implements CrossoverPolicy {

    @Override
    public ChromosomePair crossover(Chromosome chr1, Chromosome chr2) throws MathIllegalArgumentException {
        if ((chr1 instanceof BitChromosome) && (chr2 instanceof BitChromosome)) {
            BitChromosome bitsChr1 = (BitChromosome) chr1;
            BitChromosome bitsChr2 = (BitChromosome) chr2;
            BitSpindle spindle = bitsChr1.getSpindle();
            BenefitCalculator<BitSet> calculator = bitsChr1.getCalculator();

            BitSet a = bitsChr1.getBits();
            BitSet b = bitsChr2.getBits();
            spindle.randomCrossover(a, b);

            return new ChromosomePair(new BitChromosome(a, spindle, calculator),
                    new BitChromosome(b, spindle, calculator));
        }

        throw new MathIllegalArgumentException(new DummyLocalizable("bits crossover only works on BitsChromosome"));
    }
}
