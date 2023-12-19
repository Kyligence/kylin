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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.planner.util.BitUtil;

public class BitSpindle {

    private final int maxSelectNum;
    private final int maxChangeNum;
    // bits of mandatory cuboid list
    private final BitSet mandatoryBits;
    // bits of existing cuboid list including mandatory
    private final BitSet existingBits;
    // bits of cuboid list from existing and hit
    private final BitSet eliteBits;
    // cuboid space: existing and eligible
    private final List<BitSet> cuboidSpace;
    // significant bits length: exact size of cuboid space
    private final int nbits;

    private BitSpindle(int maxSelectNum, int maxChangeNum, BitSet mandatoryBits, BitSet existingBits, BitSet eliteBits,
                       List<BitSet> cuboidSpace) {
        this.maxSelectNum = maxSelectNum;
        this.maxChangeNum = maxChangeNum;
        this.cuboidSpace = cuboidSpace;
        this.mandatoryBits = mandatoryBits;
        this.existingBits = existingBits;
        this.eliteBits = eliteBits;
        this.nbits = cuboidSpace.size();
    }

    public static Builder builder() {
        return new Builder();
    }

    public List<BitSet> getCuboidList(BitSet chrBits) {
        if (chrBits == null) {
            return Collections.emptyList();
        }
        return chrBits.stream().filter(i -> i < nbits) // should never happen
                .mapToObj(cuboidSpace::get) // index -> cuboid
                .collect(Collectors.toList());
    }

    public BitSet randomEliteBits() {
        final BitSet bits = BitUtil.cloneBits(eliteBits);
        // randomly flip bit based on change limit
        for (int i = 0; i < maxChangeNum; i++) {
            bits.flip(GeneticBitUtil.randomBit(nbits));
        }
        rectify(bits);
        return bits;
    }

    public void randomMutate(BitSet bits) {
        int i = GeneticBitUtil.randomNextClearBit(mandatoryBits, nbits);
        if (i < 0) {
            return;
        }
        bits.flip(i);
        rectify(bits);
    }

    public void randomCrossover(BitSet a, BitSet b) {
        // random position: neither 0 nor (nbits - 1)
        final int p = nbits <= 2 ? 1 : (1 + GeneticBitUtil.randomBit(nbits - 2));
        // head a
        BitSet ha = BitUtil.cloneBits(a);
        ha.clear(p, nbits);
        // tail a
        BitSet ta = BitUtil.cloneBits(a);
        ta.clear(0, p);

        // head b
        BitSet hb = BitUtil.cloneBits(b);
        hb.clear(p, nbits);
        // tail b
        BitSet tb = BitUtil.cloneBits(b);
        tb.clear(0, p);

        // crossover a,b
        a.and(ha);
        a.or(tb);
        b.and(hb);
        b.or(ta);

        rectify(a);
        rectify(b);
    }

    private void rectify(BitSet bits) {
        // clear mandatory
        bits.andNot(mandatoryBits);

        // step 1: max change num
        BitSet xor = BitUtil.cloneBits(bits);
        xor.xor(existingBits);
        xor.andNot(mandatoryBits); // don't forget this op
        int c = xor.cardinality();
        while (c > maxChangeNum) {
            int i = GeneticBitUtil.randomNextSetBit(xor, nbits);
            if (i < 0)
                break;
            bits.flip(i);
            xor.clear(i);
            c--;
        }

        // step 2: max select num
        c = bits.cardinality() + mandatoryBits.cardinality();
        while (c > maxSelectNum) {
            int i = GeneticBitUtil.randomNextSetBit(bits, nbits);
            if (i < 0) {
                break;
            }
            bits.clear(i);
            c--;
        }

        // retain mandatory
        bits.or(mandatoryBits);
    }

    public static class Builder {

        private int maxSelectNum;
        private int maxChangeNum;
        private List<BitSet> cuboidList;
        private List<BitSet> mandatoryList;
        private List<BitSet> existingList;
        private List<BitSet> queryList;

        public Builder maxSelectNum(int maxSelectNum) {
            this.maxSelectNum = maxSelectNum;
            return this;
        }

        public Builder maxChangeNum(int maxChangeNum) {
            this.maxChangeNum = maxChangeNum;
            return this;
        }

        public Builder mandatoryList(List<BitSet> mandatoryList) {
            this.mandatoryList = mandatoryList;
            return this;
        }

        public Builder existingList(List<BitSet> existingList) {
            this.existingList = existingList;
            return this;
        }

        public Builder queryList(List<BitSet> queryList) {
            this.queryList = queryList;
            return this;
        }

        public Builder cuboidList(List<BitSet> cuboidList) {
            this.cuboidList = cuboidList;
            return this;
        }

        public BitSpindle build() {
            final List<BitSet> cuboidSpace = Optional.ofNullable(cuboidList).orElse(Collections.emptyList()).stream()
                    .collect(ImmutableList.toImmutableList());
            final int nbits = cuboidSpace.size();
            final Map<BitSet, Integer> cuboidIndexMap = ImmutableMap.<BitSet, Integer> builder().putAll(
                    IntStream.range(0, nbits).boxed().collect(Collectors.toMap(cuboidSpace::get, Function.identity())))
                    .build();

            // mandatory
            final BitSet mandatoryBits = bitsOf(mandatoryList, nbits, cuboidIndexMap);
            // existing
            final BitSet existingBits = bitsOf(existingList, nbits, cuboidIndexMap);
            existingBits.or(mandatoryBits);
            // eligible
            final BitSet eliteBits = bitsOf(queryList, nbits, cuboidIndexMap);
            eliteBits.or(existingBits);

            return new BitSpindle(maxSelectNum, maxChangeNum, mandatoryBits, existingBits, eliteBits, cuboidSpace);

        }

        private BitSet bitsOf(List<BitSet> cuboidList, final int nbits, final Map<BitSet, Integer> cuboidIndexMap) {
            final BitSet bits = BitUtil.emptyBits(nbits);
            if (cuboidList == null) {
                return bits;
            }
            return cuboidList.stream().map(c -> cuboidIndexMap.getOrDefault(c, -1))
                    // should never happen
                    .filter(i -> i >= 0).reduce(bits, (b, i) -> {
                        b.set(i);
                        return b;
                    }, (a, b) -> {
                        a.or(b);
                        return a;
                    });
        }
    }

}
