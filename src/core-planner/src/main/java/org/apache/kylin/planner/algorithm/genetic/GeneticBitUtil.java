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

import org.apache.commons.math3.genetics.GeneticAlgorithm;

class GeneticBitUtil {

    private GeneticBitUtil() {
    }

    public static int randomBit(int nbits) {
        return GeneticAlgorithm.getRandomGenerator().nextInt(nbits);
    }

    public static int randomNextSetBit(BitSet bits, int nbits) {
        int i = bits.nextSetBit(randomBit(nbits));
        if (i >= 0 && i < nbits) {
            return i;
        }

        i = bits.nextSetBit(0);
        return i < nbits ? i : -1;
    }

    public static int randomNextClearBit(BitSet bits, int nbits) {
        int i = bits.nextClearBit(randomBit(nbits));
        if (i >= 0 && i < nbits) {
            return i;
        }

        i = bits.nextClearBit(0);
        return i < nbits ? i : -1;
    }

}
