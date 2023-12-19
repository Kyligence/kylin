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

package org.apache.kylin.planner.plans;

import java.util.AbstractMap;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableBiMap;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableMap;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NDataLayout;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.planner.candy.LayoutCandy;
import org.apache.kylin.planner.internal.PlannerConfHelper;
import org.apache.kylin.planner.stats.PlanStats;
import org.apache.kylin.planner.util.BitUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CubePlan implements CubePlanStats, PlannerConfHelper {

    // existing model
    private final NDataModel model;
    // existing index plan
    private final IndexPlan indexPlan;
    // existing dataflow
    private final NDataflow dataflow;
    // column space, sorted, no duplicate
    private final ImmutableList<Integer> columnSpace;
    // significant bits length: exact size of column space
    private final int nbits;
    // column id -> index of column space
    private final ImmutableMap<Integer, Integer> columnIndexMap;

    // uniform measures, mutable for adding
    private final Map<String, NDataModel.Measure> funcMeasureMap;
    // plan stats
    private final PlanStats planStats;

    // names to flat table
    private final Node root;
    // column bits -> cuboid
    private final Map<BitSet, Node> mapping = Maps.newHashMap();

    public CubePlan(NDataModel model, IndexPlan indexPlan, NDataflow dataflow) {
        this.model = model;
        this.indexPlan = indexPlan;
        this.dataflow = dataflow;
        this.columnSpace = ImmutableList.<Integer> builder() //
                .addAll(model.getEffectiveCols().keySet().stream().sorted(Integer::compareTo)
                        .collect(Collectors.toList()))
                .build();
        this.nbits = columnSpace.size();
        this.columnIndexMap = ImmutableMap.<Integer, Integer> builder().putAll(
                IntStream.range(0, nbits).boxed().collect(Collectors.toMap(columnSpace::get, Function.identity())))
                .build();
        this.funcMeasureMap = model.getEffectiveMeasures().values().stream()
                .collect(Collectors.toMap(m -> m.getFunction().toString(), Function.identity()));
        this.planStats = new PlanStats();
        this.root = new Node(BitUtil.emptyBits(nbits));
    }


    public void analyze() {
        // add root
        addCuboid(root);
        // add mandatory
        addMandatory();
        // add existing
        final Map<BitSet, IndexEntity> existingMap = getCuboidIndexMap(indexPlan.getAllIndexes(false));
        existingMap.forEach((k, v) -> {
            Node cuboid = addMerge(k, Lists.newArrayList(v.getMeasureSet()));
            cuboid.setExisting();
        });

        // report update frequency
        final Map<Long, BitSet> layoutMap = existingMap.entrySet().stream().flatMap(e -> {
            final BitSet dimension = e.getKey();
            return e.getValue().getLayouts().stream().map(l -> new AbstractMap.SimpleEntry<>(l.getId(), dimension));
        }).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> v1));
        final int ceiling = ceilingUpdateFrequency();
        reportUpdateFrequency(dataflow.getSegments().stream().flatMap(s -> s.getSegDetails().getEffectiveLayouts()
                .stream().map(NDataLayout::getLayoutId).map(layoutMap::get).filter(Objects::nonNull)
                .collect(Collectors.toMap(Function.identity(), e -> (ceiling - 1), (v1, v2) -> v1)).entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> Math.max(a - 1, 0))));

        // wip: report tuple count (stats from data layout, fallback for too large flat table counting)
        // report hit count (fallback for existing model upgrade)
        reportQueryFrequency(dataflow.getLayoutHitCount().entrySet().stream().map(e -> {
            BitSet dimension = layoutMap.get(e.getKey());
            long count = e.getValue().getMap().values().stream().reduce(0, Integer::sum).longValue();
            if (dimension == null || count <= 0L) {
                return null;
            }
            return new AbstractMap.SimpleEntry<>(dimension, count);
        }).filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Long::sum)));
    }

    public NDataModel getModel() {
        return model;
    }

    public NDataflow getDataflow() {
        return dataflow;
    }

    public Map<BitSet, List<TblColRef>> getCuboidColumnMap() {
        return mapping.values().stream().filter(Node::hasAggregation).map(Node::getBits)
                .collect(ImmutableMap.toImmutableMap(Function.identity(), this::getColumnList));
    }

    public List<TblColRef> getColumnList(BitSet columnBits) {
        return columnBits.stream().mapToObj(columnSpace::get) // index -> id
                .map(model::getColRef) // id -> column
                .filter(Objects::nonNull) // should never happen
                .collect(ImmutableList.toImmutableList());
    }

    public BitSet emptyBits() {
        return BitUtil.emptyBits(nbits);
    }

    public int getNbits() {
        return nbits;
    }

    public Map<BitSet, List<NDataModel.Measure>> getCuboidMeasureMap() {
        return mapping.entrySet().stream()
                .collect(ImmutableMap.toImmutableMap(Map.Entry::getKey, e -> e.getValue().getMeasureList()));
    }

    public Map<BitSet, List<NDataModel.Measure>> nonHitCuboidMeasureMap() {
        return mapping.entrySet().stream().filter(e -> !e.getValue().isHit())
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getMeasureList()));
    }

    public void addMergeSetHit(BitSet dimension, List<NDataModel.Measure> measureList) {
        Node cuboid = addMerge(dimension, measureList);
        cuboid.setHit();
    }

    public void addMergeSetMandatory(BitSet dimension, List<NDataModel.Measure> measureList) {
        Node cuboid = addMerge(dimension, measureList);
        cuboid.setMandatory();
    }

    public Node addMerge(BitSet dimension, List<NDataModel.Measure> measureList) {
        Node node = getCuboid(dimension);
        Map<String, NDataModel.Measure> measureMap = findMeasureMap(measureList);
        if (node == null) {
            node = new Node(dimension, measureMap);
            addCuboid(node);
        } else if (!node.isRoot()) {
            node.merge(measureMap);
        }
        return node;
    }

    public void remove(BitSet dimension) {
        Node node = getCuboid(dimension);
        if (node != null && node.hasAggregation()) {
            removeCuboid(node);
        }
    }

    public Map<BitSet, TblColRef> getBitsColumnMap() {
        return model.getEffectiveCols().entrySet().stream().map(e -> {
            int i = columnIndexMap.getOrDefault(e.getKey(), -1);
            if (i < 0) {
                return null;
            }
            BitSet bits = emptyBits();
            bits.set(i);
            return new AbstractMap.SimpleEntry<>(bits, e.getValue());
        }).filter(Objects::nonNull).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public List<BitSet> getSingleDimensionBitsList() {
        return model.getEffectiveDimensions().keySet().stream().map(columnIndexMap::get).filter(Objects::nonNull)
                // should never happen
                .map(i -> {
                    BitSet bits = emptyBits();
                    bits.set(i);
                    return bits;
                }).collect(Collectors.toList());
    }

    public List<NDataModel.Measure> getMeasureList() {
        return funcMeasureMap.values().stream().sorted(Comparator.comparing(NDataModel.Measure::getId))
                .collect(Collectors.toList());
    }

    public BitSet getColumnBits(List<String> nameList) {
        return nameList.stream().map(model::getColumnIdByColumnName) // alias dot name -> id
                .map(id -> columnIndexMap.getOrDefault(id, -1)) // id -> index
                .filter(i -> i >= 0).reduce(emptyBits(), (b, i) -> {
                    b.set(i);
                    return b;
                }, (a, b) -> {
                    a.or(b);
                    return a;
                });
    }

    public void apply(List<BitSet> cuboidList) {
        mapping.values().forEach(Node::sweep);
        Optional.ofNullable(cuboidList).orElse(Collections.emptyList()).stream().map(mapping::get)
                .filter(Objects::nonNull) // should never happen
                .forEach(Node::choose);
    }

    public List<ComputedColumnDesc> computedColumnToAdd() {
        // to be continued
        return Collections.emptyList();
    }

    public List<NDataModel.NamedColumn> dimensionToAdd() {
        final ImmutableBiMap<Integer, TblColRef> dimensionMap = model.getEffectiveDimensions();
        final Map<Integer, NDataModel.NamedColumn> columnMap = model.getEffectiveNamedColumns();
        return mapping.values().stream().filter(Node::isSelected).filter(Node::hasAggregation).map(Node::getBits)
                .reduce(emptyBits(), (a, b) -> {
                    a.or(b);
                    return a;
                }).stream().mapToObj(columnSpace::get).filter(id -> !dimensionMap.containsKey(id))
                .map(columnMap::get).filter(Objects::nonNull)
                .collect(ImmutableList.toImmutableList());
    }

    public List<NDataModel.Measure> measureToAdd() {
        final ImmutableBiMap<Integer, NDataModel.Measure> measureMap = model.getEffectiveMeasures();
        return funcMeasureMap.entrySet().stream()
                .filter(e -> mapping.values().stream().filter(Node::isSelected).filter(Node::hasAggregation)
                        .anyMatch(c -> c.hasMeasure(e.getKey())))
                .filter(e -> !measureMap.containsKey(e.getValue().getId())).map(Map.Entry::getValue)
                .collect(ImmutableList.toImmutableList());

    }

    public List<LayoutCandy> layoutToAdd(NDataModel targetModel) {
        final Map<Integer, NDataModel.NamedColumn> columnMap = model.getEffectiveNamedColumns();
        return mapping.values().stream().filter(Node::isSelected).filter(n -> !n.isExisting()).map(node -> {
            List<Integer> dimensionList = node.getBits().stream().mapToObj(columnSpace::get).map(columnMap::get)
                    // should never happen
                    .filter(Objects::nonNull)
                    .map(c0 -> targetModel.getAllNamedColumns().stream().filter(NDataModel.NamedColumn::isExist)
                            .filter(c1 -> Objects.equals(c0.getAliasDotColumn(), c1.getAliasDotColumn())).findAny()
                            .orElseThrow(() -> new IllegalStateException(c0.getAliasDotColumn())).getId())
                    .collect(ImmutableList.toImmutableList());
            List<Integer> measureList = node.getMeasureList().stream()
                    .map(m0 -> targetModel.getAllMeasures().stream().filter(m -> !m.isTomb())
                            .filter(m1 -> Objects.equals(m0.getFunction().toString(), m1.getFunction().toString()))
                            .findAny().orElseThrow(() -> new IllegalStateException(m0.getFunction().toString()))
                            .getId())
                    .collect(ImmutableList.toImmutableList());
            // wip: sort by, shard by
            return new LayoutCandy(dimensionList, measureList);
                }).collect(ImmutableList.toImmutableList());
    }


    public List<LayoutEntity> layoutToRemove() {
        final Map<BitSet, IndexEntity> cuboidIndexMap = getCuboidIndexMap(indexPlan.getAllIndexes(false));
        return mapping.values().stream().filter(Node::isExisting).filter(n -> !n.isSelected())
                .map(n -> cuboidIndexMap.get(n.getBits()))
                .filter(Objects::nonNull).flatMap(index -> index.getLayouts().stream())
                .collect(ImmutableList.toImmutableList());
    }

    private void addCuboid(Node cuboid) {
        mapping.put(cuboid.getBits(), cuboid);
    }

    private void removeCuboid(Node cuboid) {
        mapping.remove(cuboid.getBits());
    }

    private Node getCuboid(BitSet bits) {
        return mapping.get(bits);
    }

    private void addMandatory() {
        final Map<BitSet, IndexEntity> mandatoryMap = getCuboidIndexMap(
                Lists.newArrayList(indexPlan.getPlannerWhiteList().stream().map(indexPlan::getLayoutEntity)
                        .filter(Objects::nonNull).map(LayoutEntity::getIndex)
                        .collect(Collectors.toMap(IndexEntity::getId, Function.identity(), (v1, v2) -> v1)).values()));
        mandatoryMap.forEach((k, v) -> {
            Node cuboid = addMerge(k, Lists.newArrayList(v.getMeasureSet()));
            cuboid.setMandatory();
        });
    }

    private Map<String, NDataModel.Measure> findMeasureMap(List<NDataModel.Measure> measureList) {
        Map<String, NDataModel.Measure> measureMap = Maps.newHashMap();
        for (NDataModel.Measure measure : measureList) {
            // wip: cc expr should have been handled
            if (measure.getFunction().getParameters().stream()
                    .noneMatch(p -> p.isColumnType() && model.getColumnIdByColumnName(p.getValue()) < 0)) {
                measure.getFunction().init(model); // essential operation: complete data type
                String funcName = measure.getFunction().toString();
                measureMap.putIfAbsent(funcName, funcMeasureMap.computeIfAbsent(funcName, f -> measure));
            }
        }
        return measureMap;
    }

    private Map<BitSet, IndexEntity> getCuboidIndexMap(Collection<IndexEntity> indexList) {
        Map<BitSet, IndexEntity> map = Maps.newHashMap();
        for (IndexEntity index : indexList) {
            // wip: considering table index
            if (!index.isTableIndex()) {
                BitSet bits = index.getDimensions().stream().map(columnIndexMap::get).reduce(emptyBits(), (b, i) -> {
                    b.set(i);
                    return b;
                }, (a, b) -> {
                    a.or(b);
                    return a;
                });
                map.putIfAbsent(bits, index);
            }
        }
        return map;
    }

    @Override
    public void reportQueryFrequency(Map<BitSet, Long> queryFreqMap) {
        planStats.mergeQueryFrequency(queryFreqMap);
    }

    @Override
    public void reportUpdateFrequency(Map<BitSet, Integer> updateFreqMap) {
        planStats.mergeUpdateFrequency(updateFreqMap);
    }

    @Override
    public void reportTupleCount(Map<BitSet, Long> countMap) {
        planStats.mergeTupleCount(countMap);
    }

    @Override
    public List<BitSet> cuboidList() {
        return mapping.values().stream().filter(Node::hasAggregation).map(Node::getBits)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<BitSet> mandatoryList() {
        return mapping.values().stream().filter(Node::hasAggregation).filter(Node::isMandatory).map(Node::getBits)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<BitSet> existingList() {
        return mapping.values().stream().filter(Node::hasAggregation).filter(Node::isExisting).map(Node::getBits)
                .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<BitSet> queryList() {
        return planStats.queryList();
    }

    @Override
    public BitSet ceilingBits() {
        return root.getBits();
    }

    @Override
    public long ceilingRowCount() {
        Long count = planStats.getTupleCount(root.getBits());
        if (count == null) {
            throw new IllegalStateException("Root row count should not be empty.");
        }
        return count;
    }

    @Override
    public int ceilingUpdateFrequency() {
        return dataflow.getSegments().getSegRanges().size();
    }

    @Override
    public long tupleCount(BitSet cuboid, long ceiling) {
        Long count = planStats.getTupleCount(cuboid);
        if (count == null) {
            return ceiling;
        }
        return count;
    }

    @Override
    public long queryFrequency(BitSet cuboid) {
        Long freq = planStats.getQueryFrequency(cuboid);
        if (freq == null) {
            return 0L;
        }
        return freq;
    }

    @Override
    public int updateFrequency(BitSet cuboid, int ceiling) {
        Integer freq = planStats.getUpdateFrequency(cuboid);
        if (freq == null) {
            return ceiling;
        }
        return freq;
    }

    public final String treeString() {
        // wip: complete this
        return "to be continued";
    }

    // -------------------------------------------------------------

    private static class Node extends TreeNode<Node> {

        private final BitSet bits;
        private final Map<String, NDataModel.Measure> measureMap;
        private boolean isMandatory;
        private boolean isExisting;
        private boolean isSelected;
        private boolean isHit;

        private Node(BitSet bits) {
            // father more empty bits representing for flat table
            this(bits, Collections.emptyMap());
        }

        private Node(BitSet bits, Map<String, NDataModel.Measure> measureMap) {
            // aggregation cuboid
            this.bits = BitUtil.cloneBits(bits);
            this.measureMap = Maps.newHashMap(measureMap);
        }

        @Override
        public boolean isRoot() {
            return bits.nextSetBit(0) < 0;
        }

        private BitSet getBits() {
            // hidden internal mutable bits
            return BitUtil.cloneBits(bits);
        }

        private List<NDataModel.Measure> getMeasureList() {
            return measureMap.values().stream().sorted(Comparator.comparing(NDataModel.Measure::getId))
                    .collect(ImmutableList.toImmutableList());
        }

        private boolean hasMeasure(String key) {
            return measureMap.containsKey(key);
        }

        private boolean hasAggregation() {
            return measureMap != null && !measureMap.isEmpty();
        }

        private void merge(Map<String, NDataModel.Measure> measureMap) {
            this.measureMap.putAll(measureMap);
        }

        private boolean isMandatory() {
            return isMandatory;
        }

        private void setMandatory() {
            this.isMandatory = true;
        }

        private void setExisting() {
            this.isExisting = true;
        }

        private boolean isExisting() {
            return isExisting;
        }

        private void setHit() {
            this.isHit = true;
        }

        private boolean isHit() {
            return isHit;
        }

        private boolean isSelected() {
            return isSelected;
        }

        private void sweep() {
            this.isSelected = false;
        }

        private void choose() {
            this.isSelected = true;
        }

    }
}
