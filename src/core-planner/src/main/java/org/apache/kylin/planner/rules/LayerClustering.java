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

package org.apache.kylin.planner.rules;

import java.util.AbstractMap;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.math3.ml.clustering.Clusterable;
import org.apache.commons.math3.ml.clustering.KMeansPlusPlusClusterer;
import org.apache.commons.math3.ml.distance.CanberraDistance;
import org.apache.commons.math3.ml.distance.ChebyshevDistance;
import org.apache.commons.math3.ml.distance.DistanceMeasure;
import org.apache.commons.math3.ml.distance.EarthMoversDistance;
import org.apache.commons.math3.ml.distance.EuclideanDistance;
import org.apache.commons.math3.ml.distance.ManhattanDistance;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.planner.plans.CubePlan;
import org.apache.kylin.planner.util.BitUtil;

public class LayerClustering extends Rule<CubePlan> {

    private static final int MAX_CLUSTERING_TIMES = 32;

    private final List<DistanceMeasure> distanceList = ImmutableList.<DistanceMeasure> builder()
            .add(new CanberraDistance(), new ChebyshevDistance(), new EarthMoversDistance(), new EuclideanDistance(),
                    new ManhattanDistance())
            .build();

    @Override
    public CubePlan apply(CubePlan plan) {
        final int nbits = plan.getNbits();
        final List<BitSet> cuboidList = plan.cuboidList();
        final int ceiling = cuboidList.stream().reduce(plan.emptyBits(), (a, b) -> {
            a.or(b);
            return a;
        }).cardinality();

        final List<Integer> fibList = fibonacciList(ceiling);
        Map<Integer, Set<BitSet>> cardCuboidMap = cuboidList.stream()
                .collect(ImmutableMap.toImmutableMap(BitSet::cardinality, Sets::newHashSet, (a, b) -> {
                    a.addAll(b);
                    return a;
                }));
        final int fibListSize = fibList.size();
        // skip 0, 1, 1
        for (int i = 3; i < fibListSize; i++) {
            final int belowDimCount = fibList.get(i - 2);
            final int thisDimCount = fibList.get(i);
            final int aboveDimCount = (i + 2) > (fibListSize - 1) ? fibList.get(fibListSize - 1) : fibList.get(i + 2);
            final List<Point> pointList = cardCuboidMap.entrySet().stream()
                    .filter(e -> belowDimCount <= e.getKey() && e.getKey() < thisDimCount)
                    .flatMap(e -> e.getValue().stream()).map(b -> new Point(nbits, b))
                    .collect(ImmutableList.toImmutableList());

            if (pointList.isEmpty()) {
                continue;
            }

            final int j = fibListSize - i + 1;
            final int k = clusteringK(j, pointList.size(), fibList);

            int clusteringTimes = MAX_CLUSTERING_TIMES;
            while (clusteringTimes-- > 0) {
                final Map<Integer, Set<BitSet>> clusterMap = distanceList.parallelStream()
                        .flatMap(distance -> new KMeansPlusPlusClusterer<Point>(k, 200, distance).cluster(pointList)
                                .stream().map(c -> c.getPoints().stream().map(Point::getCuboid).reduce(plan.emptyBits(),
                                        (a, b) -> {
                                            a.or(b);
                                            return a;
                                        }))
                                .map(e -> new AbstractMap.SimpleEntry<>(e.cardinality(), e)))
                        .filter(e -> e.getKey() > 0)
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> Sets.newHashSet(e.getValue()),
                                (a, b) -> {
                                    a.addAll(b);
                                    return a;
                                }));

                cardCuboidMap = Stream.of(cardCuboidMap, clusterMap).flatMap(m -> m.entrySet().stream())
                        .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> {
                            a.addAll(b);
                            return a;
                        }));
                // wip: count limitation
                if ((k << 7) < cardCuboidMap.entrySet().stream()
                        .filter(e -> thisDimCount <= e.getKey() && e.getKey() < aboveDimCount).map(Map.Entry::getValue)
                        .map(Set::size).reduce(0, Integer::sum)) {
                    break;
                }
            }
        }

        addToPlan(plan, cardCuboidMap.values().stream().flatMap(Set::stream).collect(Collectors.toList()));
        return plan;
    }

    private int clusteringK(int j, int pointListSize, List<Integer> fibList) {
        final int idol = j > (fibList.size() - 1) ? fibList.get(fibList.size() - 1) : fibList.get(j);
        return pointListSize < (idol << 1) ? Math.max(1, (pointListSize >> 1)) : idol;
    }

    private void addToPlan(CubePlan plan, List<BitSet> cuboidList) {
        Map<BitSet, List<NDataModel.Measure>> cuboidMeasureMap = plan.getCuboidMeasureMap();
        for (BitSet cuboid : cuboidList) {
            List<NDataModel.Measure> measureList = cuboidMeasureMap.entrySet().stream()
                    .filter(e -> BitUtil.isChild(cuboid, e.getKey())).map(Map.Entry::getValue).flatMap(List::stream)
                    .collect(Collectors.toList());
            plan.addMerge(cuboid, measureList);
        }
    }

    private List<Integer> fibonacciList(int ceiling) {
        // 0, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, ...
        List<Integer> fibList = Lists.newArrayList(0, 1);
        int i = 2;
        int fi = 1;
        while (fi <= ceiling) {
            fi = fibList.get(i - 1) + fibList.get(i - 2);
            fibList.add(fi);
            i++;
        }
        return ImmutableList.copyOf(fibList);
    }

    private static class Point implements Clusterable {

        private final int nbits;
        private final BitSet cuboid;

        public Point(int nbits, BitSet cuboid) {
            this.nbits = nbits;
            this.cuboid = cuboid;
        }

        @Override
        public double[] getPoint() {
            final double[] arr = new double[nbits];
            cuboid.stream().forEach(i -> arr[i] = 1.0d);
            return arr;
        }

        public BitSet getCuboid() {
            return cuboid;
        }
    }

}
