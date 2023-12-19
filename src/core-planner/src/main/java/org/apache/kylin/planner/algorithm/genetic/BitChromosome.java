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
import java.util.Objects;

import org.apache.commons.math3.genetics.Chromosome;
import org.apache.kylin.planner.algorithm.BenefitCalculator;
import org.apache.kylin.planner.util.BitUtil;

public class BitChromosome extends Chromosome {

    private final BitSet bits;
    private final BitSpindle spindle;
    private final BenefitCalculator<BitSet> calculator;

    public BitChromosome(BitSet bits, BitSpindle spindle, BenefitCalculator<BitSet> calculator) {
        this.bits = bits;
        this.spindle = spindle;
        this.calculator = calculator;
    }

    public BitSet getBits() {
        return BitUtil.cloneBits(bits);
    }

    public BitSpindle getSpindle() {
        return spindle;
    }

    public BenefitCalculator<BitSet> getCalculator() {
        return calculator;
    }

    @Override
    public double fitness() {
        return calculator.benefitOf(spindle.getCuboidList(bits));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other)
            return true;

        if (other == null || getClass() != other.getClass())
            return false;

        return Objects.equals(bits, ((BitChromosome) other).getBits());
    }

    @Override
    public int hashCode() {
        return bits == null ? 0 : bits.hashCode();
    }

    @Override
    protected boolean isSame(Chromosome other) {
        if ((other instanceof BitChromosome)) {
            return Objects.equals(bits, ((BitChromosome) other).getBits());
        }
        return false;
    }
}
