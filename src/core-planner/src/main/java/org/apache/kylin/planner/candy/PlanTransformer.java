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

package org.apache.kylin.planner.candy;

import static org.apache.kylin.metadata.recommendation.candidate.RawRecItem.RawRecType.ADDITIONAL_LAYOUT;

import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Sets;
import org.apache.kylin.metadata.cube.model.IndexEntity;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.optimization.FrequencyMap;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.recommendation.candidate.LayoutMetric;
import org.apache.kylin.metadata.recommendation.candidate.RawRecItem;
import org.apache.kylin.metadata.recommendation.candidate.RawRecManager;
import org.apache.kylin.metadata.recommendation.entity.DimensionRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.LayoutRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.MeasureRecItemV2;
import org.apache.kylin.metadata.recommendation.entity.RecItemV2;
import org.apache.kylin.metadata.recommendation.util.RawRecUtil;
import org.apache.kylin.planner.plans.CubePlan;

public class PlanTransformer {

    private final CubePlan plan;
    private final String project;
    private final String modelId;
    private final int semanticVersion;
    private final RawRecManager recManager;
    private final NDataModelManager modelManager;
    private final long timeVersion;
    private final Map<String, RawRecItem> flagRecItemMap;
    private final Map<String, List<String>> md5FlagMap;
    private final Consumer<NDataModel> modelInitializer;

    public PlanTransformer(CubePlan plan, KylinConfig config) {
        this.plan = plan;
        this.project = plan.getModel().getProject();
        this.modelId = plan.getModel().getId();
        this.semanticVersion = plan.getModel().getSemanticVersion();
        this.recManager = RawRecManager.getInstance(project);
        this.modelManager = NDataModelManager.getInstance(config, project);
        this.timeVersion = System.currentTimeMillis();
        this.flagRecItemMap = recManager.queryNonLayoutRecItems(Sets.newHashSet(modelId));
        this.md5FlagMap = RawRecUtil.uniqueFlagsToMd5Map(flagRecItemMap.keySet());
        this.modelInitializer = m -> m.init(config, project, modelManager.getCCRelatedModels(plan.getModel()), true);
    }

    public void transformAndSave() {
        final NDataModel model = modelManager.getDataModelDescWithoutInit(modelId);
        // essential preparation
        modelInitializer.accept(model);

        List<ComputedColumnDesc> ccList = plan.computedColumnToAdd();
        List<NDataModel.NamedColumn> columnList = plan.dimensionToAdd();
        List<NDataModel.Measure> measureList = plan.measureToAdd();

        addCCToModel(model, ccList);
        addDimensionToModel(model, columnList);
        addMeasureToModel(model, measureList);

        // activate dimension, measure
        modelInitializer.accept(model);

        addCCRecItem(model, ccList);
        addDimensionRecItem(model, columnList);
        addMeasureRecItem(model, measureList);
        addLayoutRecItem(model, plan.layoutToAdd(model));
        removeLayoutRecItem(plan.layoutToRemove());
    }

    private void addCCToModel(NDataModel model, List<ComputedColumnDesc> ccList) {
        Objects.requireNonNull(model);
        if (ccList == null || ccList.isEmpty()) {
            // do nothing
            return;
        }
        throw new UnsupportedOperationException("Cannot add computed column to model.");
    }

    private void addDimensionToModel(NDataModel model, List<NDataModel.NamedColumn> columnList) {
        for (NDataModel.NamedColumn column : columnList) {
            NDataModel.NamedColumn findOne = model.getAllNamedColumns().stream()
                    .filter(c -> Objects.equals(c.getId(), column.getId()))
                    // double check
                    .filter(c -> Objects.equals(c.getAliasDotColumn(), column.getAliasDotColumn())).findFirst()
                    // should never happen
                    .orElseThrow(() -> new IllegalStateException(column.getAliasDotColumn()));
            findOne.setStatus(NDataModel.ColumnStatus.DIMENSION);
        }
    }

    private void addMeasureToModel(NDataModel model, List<NDataModel.Measure> measureList) {
        // measure id counter
        int maxMeasureId = model.getMaxMeasureId();
        for (NDataModel.Measure measure : measureList) {
            // handle parameters
            measure.getFunction().getParameters().stream().filter(ParameterDesc::isColumnType).forEach(desc -> {
                NDataModel.NamedColumn findOne = model.getAllNamedColumns().stream()
                        .filter(c -> c.getAliasDotColumn().equalsIgnoreCase(desc.getValue())).findAny()
                        .orElseThrow(() -> new IllegalStateException(desc.getValue()));
                desc.setValue(findOne.getAliasDotColumn());
            });

            // handle data type
            measure.getFunction().init(model);

            measure.setId(++maxMeasureId);
            model.getAllMeasures().add(measure);
        }
    }

    private void addCCRecItem(NDataModel model, List<ComputedColumnDesc> ccList) {
        Objects.requireNonNull(model);
        if (ccList == null || ccList.isEmpty()) {
            return;
        }
        throw new UnsupportedOperationException("Cannot create computed column recommendation.");
    }

    private void addDimensionRecItem(NDataModel model, List<NDataModel.NamedColumn> columnList) {
        final Map<String, ComputedColumnDesc> ccMap = model.getCcMap();
        final Map<String, RawRecItem> uuidRecItemMap = flagRecItemMap.values().stream()
                .collect(Collectors.toMap(e -> e.getRecEntity().getUuid(), Function.identity()));
        for (NDataModel.NamedColumn column : columnList) {
            TblColRef colRef = model.getEffectiveCols().get(column.getId());
            String uniqueContent = RawRecUtil.dimensionUniqueContent(colRef, ccMap, Collections.emptySet());
            DimensionRecItemV2 dimItem = new DimensionRecItemV2(column, colRef, uniqueContent);

            checkAndSaveRecItem(dimItem, uuidRecItemMap, model, RawRecItem.RawRecType.DIMENSION, null);
        }
    }

    private void addMeasureRecItem(NDataModel model, List<NDataModel.Measure> measureList) {
        final Map<String, ComputedColumnDesc> ccMap = model.getCcMap();
        final Map<String, RawRecItem> uuidRecItemMap = flagRecItemMap.values().stream()
                .collect(Collectors.toMap(e -> e.getRecEntity().getUuid(), Function.identity()));
        for (NDataModel.Measure measure : measureList) {
            String uniqueContent = RawRecUtil.measureUniqueContent(measure, ccMap, Collections.emptySet());
            MeasureRecItemV2 measureItem = new MeasureRecItemV2();
            measureItem.setMeasure(measure);
            measureItem.setCreateTime(System.currentTimeMillis());
            measureItem.setUniqueContent(uniqueContent);
            measureItem.setUuid(String.format(Locale.ROOT, "measure_%s", RandomUtil.randomUUIDStr()));

            checkAndSaveRecItem(measureItem, uuidRecItemMap, model, RawRecItem.RawRecType.MEASURE, null);
        }
    }

    private void addLayoutRecItem(NDataModel model, List<LayoutCandy> candyList) {
        final Map<String, RawRecItem> layoutFlagRecItemMap = recManager.queryNonAppliedLayoutRawRecItems(modelId, true);
        for (LayoutCandy candy : candyList) {
            final LayoutRecItemV2 layoutItem = createLayoutRecItem(candy);
            // mapping to dependency
            layoutItem.updateLayoutContent(model, flagRecItemMap, Collections.emptySet());

            checkAndSaveRecItem(layoutItem, null, null, ADDITIONAL_LAYOUT, layoutFlagRecItemMap);
        }
    }
    
    private void checkAndSaveRecItem(RecItemV2 recItem, Map<String, RawRecItem> uuidRecItemMap, NDataModel model,
            RawRecItem.RawRecType recType, Map<String, RawRecItem> layoutFlagRecItemMap) {
        String content = RawRecUtil.getContent(project, modelId, recItem.getUniqueContent(), recType);
        String md5 = RawRecUtil.computeMD5(content);

        AtomicBoolean retry = new AtomicBoolean(false);
        JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
            Pair<String, RawRecItem> md5RecItemPair;
            if (retry.get()) {
                md5RecItemPair = recManager.queryRecItemByMd5(md5, content);
            } else {
                retry.set(true);
                md5RecItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, md5FlagMap,
                        layoutFlagRecItemMap != null ? layoutFlagRecItemMap : flagRecItemMap);
            }
            RawRecItem item;
            if (md5RecItemPair.getSecond() == null) {
                item = new RawRecItem(project, modelId, semanticVersion, recType);
                item.setUniqueFlag(md5RecItemPair.getFirst());
                item.setCreateTime(recItem.getCreateTime());
                item.setUpdateTime(recItem.getCreateTime());
                item.setState(RawRecItem.RawRecState.INITIAL);
                item.setRecEntity(recItem);
            } else {
                item = md5RecItemPair.getSecond();
                item.setUpdateTime(System.currentTimeMillis());
            }
            item.setDependIDs(recItem.genDependIds(uuidRecItemMap, recItem.getUniqueContent(), model));
            if (recType == ADDITIONAL_LAYOUT) {
                item.setRecSource(RawRecItem.INDEX_PLANNER);
                item.setState(RawRecItem.RawRecState.RECOMMENDED);
            }
            recManager.saveOrUpdate(item);
            if (recType != ADDITIONAL_LAYOUT) {
                flagRecItemMap.put(item.getUniqueFlag(), item);
            }
            return null;
        });
    }

    private LayoutRecItemV2 createLayoutRecItem(LayoutCandy candy) {
        List<Integer> colOrder = ImmutableList.<Integer> builder().addAll(candy.getDimensionList())
                .addAll(candy.getMeasureList()).build();
        LayoutEntity layout = new LayoutEntity();
        layout.setColOrder(colOrder);
        layout.setSortByColumns(candy.getSortBy());
        layout.setShardByColumns(candy.getShardBy());
        layout.setAuto(true);

        LayoutRecItemV2 layoutItem = new LayoutRecItemV2();
        layoutItem.setLayout(layout);
        layoutItem.setCreateTime(timeVersion);
        layoutItem.setAgg(!candy.getMeasureList().isEmpty());
        layoutItem.setUuid(RandomUtil.randomUUIDStr());
        return layoutItem;
    }

    public void removeLayoutRecItem(List<LayoutEntity> layoutList) {
        final Map<String, RawRecItem> layoutFlagRecItemMap = recManager.queryNonAppliedLayoutRawRecItems(modelId,
                false);
        final Map<String, List<String>> layoutMd5FlagMap = RawRecUtil
                .uniqueFlagsToMd5Map(layoutFlagRecItemMap.keySet());
        final Map<Long, FrequencyMap> layoutHitCountMap = plan.getDataflow().getLayoutHitCount();
        final AtomicInteger recCounter = new AtomicInteger(0);
        for (LayoutEntity layout : layoutList) {
            final String content = RawRecUtil.getContent(project, modelId, layout.genUniqueContent(),
                    RawRecItem.RawRecType.REMOVAL_LAYOUT);
            final String md5 = RawRecUtil.computeMD5(content);
            final AtomicBoolean retry = new AtomicBoolean(false);
            JdbcUtil.withTxAndRetry(recManager.getTransactionManager(), () -> {
                Pair<String, RawRecItem> recItemPair;
                if (retry.get()) {
                    recItemPair = recManager.queryRecItemByMd5(md5, content);
                } else {
                    retry.set(true);
                    recItemPair = RawRecUtil.getRawRecItemFromMap(md5, content, layoutMd5FlagMap, layoutFlagRecItemMap);
                }
                FrequencyMap freqMap = layoutHitCountMap.getOrDefault(layout.getId(), new FrequencyMap());
                RawRecItem recItem = createRawRecItem(recItemPair, layout, freqMap, recCounter);
                if (recItem.getLayoutMetric() != null) {
                    RawRecManager.getInstance(project).saveOrUpdate(recItem);
                }
                return null;
            });
        }
    }

    private RawRecItem createRawRecItem(Pair<String, RawRecItem> recItemPair, LayoutEntity layout, FrequencyMap freqMap,
            AtomicInteger recCounter) {
        RawRecItem recItem;
        if (recItemPair.getSecond() == null) {
            LayoutRecItemV2 item = new LayoutRecItemV2();
            item.setLayout(layout);
            item.setCreateTime(timeVersion);
            item.setAgg(layout.getId() < IndexEntity.TABLE_INDEX_START_ID);
            item.setUuid(RandomUtil.randomUUIDStr());

            recItem = new RawRecItem(project, modelId, semanticVersion, RawRecItem.RawRecType.REMOVAL_LAYOUT);
            recItem.setRecEntity(item);
            recItem.setCreateTime(timeVersion);
            recItem.setUpdateTime(timeVersion);
            recItem.setState(RawRecItem.RawRecState.INITIAL);
            recItem.setUniqueFlag(recItemPair.getFirst());
            recItem.setDependIDs(item.genDependIds());
            recItem.setLayoutMetric(new LayoutMetric(freqMap, new LayoutMetric.LatencyMap()));
            recItem.setRecSource(RawRecItem.INDEX_PLANNER);
            recCounter.getAndIncrement();
        } else {
            recItem = recItemPair.getSecond();
            recItem.setUpdateTime(timeVersion);
            recItem.setRecSource(RawRecItem.INDEX_PLANNER);
            if (recItem.getState() == RawRecItem.RawRecState.DISCARD) {
                recItem.setState(RawRecItem.RawRecState.INITIAL);
                LayoutMetric layoutMetric = recItem.getLayoutMetric();
                if (layoutMetric == null) {
                    recItem.setLayoutMetric(new LayoutMetric(freqMap, new LayoutMetric.LatencyMap()));
                } else {
                    layoutMetric.setFrequencyMap(freqMap);
                }
            }
        }
        return recItem;
    }

}
