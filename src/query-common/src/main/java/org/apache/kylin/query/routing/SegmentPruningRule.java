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

package org.apache.kylin.query.routing;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.exception.KylinTimeoutException;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.cube.cuboid.NLayoutCandidate;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ISourceAware;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.NDataModelManager;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.realization.CapabilityResult;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.exception.UserStopQueryException;
import org.apache.kylin.query.relnode.OlapContext;
import org.apache.kylin.query.util.QueryInterruptChecker;
import org.apache.kylin.query.util.RexUtils;

import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SegmentPruningRule extends PruningRule {

    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}");
    private static final Pattern TIMESTAMP_PATTERN = Pattern
            .compile("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}(\\.\\d*[1-9])?");

    @Override
    public void apply(Candidate candidate) {
        List<IRealization> realizations = candidate.getRealization().getRealizations();
        for (IRealization realization : realizations) {
            NDataflow df = (NDataflow) realization;
            Segments<NDataSegment> prunedSegments = pruneSegments(df, candidate.getCtx());
            candidate.setPrunedSegments(prunedSegments, df);
        }
        if (CollectionUtils.isEmpty(candidate.getQueryableSeg().getBatchSegments())
                && CollectionUtils.isEmpty(candidate.getQueryableSeg().getStreamingSegments())) {
            log.info("{}({}/{}): there is no queryable segments to answer this query.", this.getClass().getName(),
                    candidate.getRealization().getProject(), candidate.getRealization().getCanonicalName());
            CapabilityResult capability = new CapabilityResult();
            capability.setCapable(true);
            capability.setSelectedCandidate(NLayoutCandidate.EMPTY);
            capability.setSelectedStreamingCandidate(NLayoutCandidate.EMPTY);
            candidate.setCapability(capability);
        }
    }

    public Segments<NDataSegment> pruneSegments(NDataflow dataflow, OlapContext olapContext) {
        Segments<NDataSegment> allReadySegments = dataflow.getQueryableSegments();
        if (!NProjectManager.getProjectConfig(dataflow.getProject()).isHeterogeneousSegmentEnabled()) {
            return allReadySegments;
        }

        // pruner segment by partition column and dataformat
        PartitionDesc partitionCol = getPartitionDesc(dataflow, olapContext);
        if (isFullBuildModel(partitionCol)) {
            log.info("No partition column or partition column format is null.");
            return allReadySegments;
        }

        // pruner segment by simplify sql filter
        val relOptCluster = olapContext.getFirstTableScan().getCluster();
        val rexSimplify = new RexSimplify(relOptCluster.getRexBuilder(), RelOptPredicateList.EMPTY, true,
                relOptCluster.getPlanner().getExecutor());

        // this step may spend too long time, should be handled by https://olapio.atlassian.net/browse/KE-42295
        RexNode simplifiedSqlFilter = rexSimplify.simplifyAnds(olapContext.getExpandedFilterConditions());
        if (simplifiedSqlFilter.isAlwaysFalse()) {
            log.info("SQL filter condition is always false, pruning all ready segments");
            olapContext.getStorageContext().setFilterCondAlwaysFalse(true);
            return Segments.empty();
        }

        // pruner segment by customized scene optimize
        val partitionColRef = partitionCol.getPartitionDateColumnRef();
        if (canPruneSegmentsForMaxMeasure(dataflow, olapContext, partitionColRef)) {
            return selectSegmentsForMaxMeasure(dataflow);
        }

        if (!olapContext.getFilterColumns().contains(partitionColRef)) {
            log.info("Filter columns do not contain partition column");
            return allReadySegments;
        }

        if (simplifiedSqlFilter.isAlwaysTrue()) {
            log.info("SQL filter condition is always true, pruning no segment");
            return allReadySegments;
        }

        // prune segments by partition filter
        RexInputRef partitionColInputRef = RexUtils.transformColumn2RexInputRef(partitionColRef,
                olapContext.getAllTableScans());
        Segments<NDataSegment> selectedSegments = pruneSegmentsByPartitionFilter(dataflow, olapContext, rexSimplify,
                partitionColInputRef, simplifiedSqlFilter);
        log.info("Scan segment.size: {} after segment pruning", selectedSegments.size());
        return selectedSegments;
    }

    private Segments<NDataSegment> pruneSegmentsByPartitionFilter(NDataflow dataflow, OlapContext olapContext,
            RexSimplify rexSimplify, RexInputRef partitionColInputRef, RexNode simplifiedSqlFilter) {
        Segments<NDataSegment> selectedSegments = new Segments<>();
        PartitionDesc partitionCol = getPartitionDesc(dataflow, olapContext);
        RexBuilder rexBuilder = olapContext.getFirstTableScan().getCluster().getRexBuilder();

        for (NDataSegment dataSegment : dataflow.getQueryableSegments()) {
            try {
                QueryInterruptChecker.checkQueryCanceledOrThreadInterrupted(
                        "Interrupted during pruning segments by partition filter!",
                        "pruning segments by partition filter");

                // To improve this simplification after fixed https://olapio.atlassian.net/browse/KE-42295
                // evaluate segment range [startTime, endTime)
                val segmentRanges = transformSegment2RexCall(dataSegment, partitionCol.getPartitionDateFormat(),
                        rexBuilder, partitionColInputRef, partitionCol.getPartitionDateColumnRef().getType(),
                        dataflow.isStreaming());
                // compare with segment start
                val segmentStartPredicate = RelOptPredicateList.of(rexBuilder,
                        Lists.newArrayList(segmentRanges.getFirst()));
                var simplifiedWithPredicate = rexSimplify.withPredicates(segmentStartPredicate)
                        .simplify(simplifiedSqlFilter);
                if (simplifiedWithPredicate.isAlwaysFalse()) {
                    continue;
                }
                // compare with segment end
                val segmentEndPredicate = RelOptPredicateList.of(rexBuilder,
                        Lists.newArrayList(segmentRanges.getSecond()));
                simplifiedWithPredicate = rexSimplify.withPredicates(segmentEndPredicate)
                        .simplify(simplifiedWithPredicate);
                if (!simplifiedWithPredicate.isAlwaysFalse()) {
                    selectedSegments.add(dataSegment);
                }

            } catch (InterruptedException ie) {
                log.error(
                        String.format(Locale.ROOT, "Interrupted on pruning segments from %s!", dataSegment.toString()),
                        ie);
                Thread.currentThread().interrupt();
                throw new KylinRuntimeException(ie);
            } catch (UserStopQueryException | KylinTimeoutException e) {
                log.error(String.format(Locale.ROOT, "Stop pruning segments from %s!", dataSegment.toString()), e);
                throw e;
            } catch (Exception ex) {
                log.warn(String.format(Locale.ROOT, "To skip the exception on pruning segment %s!",
                        dataSegment.toString()), ex);
                selectedSegments.add(dataSegment);
            }
        }
        return selectedSegments;
    }

    private boolean isFullBuildModel(PartitionDesc partitionCol) {
        return PartitionDesc.isEmptyPartitionDesc(partitionCol) || partitionCol.getPartitionDateFormat() == null;
    }

    private Segments<NDataSegment> selectSegmentsForMaxMeasure(NDataflow dataflow) {
        Segments<NDataSegment> selectedSegments = new Segments<>();
        long days = dataflow.getConfig().getMaxMeasureSegmentPrunerBeforeDays();
        // segment was sorted
        Segments<NDataSegment> allReadySegments = dataflow.getQueryableSegments();
        long maxDt = allReadySegments.getLatestReadySegment().getTSRange().getEnd();
        long minDt = maxDt - DateUtils.MILLIS_PER_DAY * days;
        for (int i = allReadySegments.size() - 1; i >= 0; i--) {
            if (allReadySegments.get(i).getTSRange().getEnd() > minDt) {
                selectedSegments.add(allReadySegments.get(i));
            } else {
                break;
            }
        }
        log.info("Scan segment size: {} after max measure segment pruner. The before days: {}. Passed on segment: {}",
                selectedSegments.size(), days,
                selectedSegments.stream().map(ISegment::getName).collect(Collectors.joining(",")));
        return selectedSegments;
    }

    private boolean canPruneSegmentsForMaxMeasure(NDataflow dataflow, OlapContext olapContext,
            TblColRef partitionColRef) {
        if (dataflow.getConfig().getMaxMeasureSegmentPrunerBeforeDays() < 0) {
            return false;
        }

        if (CollectionUtils.isNotEmpty(olapContext.getGroupByColumns())
                && !olapContext.getGroupByColumns().stream().allMatch(partitionColRef::equals)) {
            return false;
        }

        if (CollectionUtils.isEmpty(olapContext.getAggregations())) {
            return false;
        }

        for (FunctionDesc agg : olapContext.getAggregations()) {
            if (FunctionDesc.FUNC_MAX.equalsIgnoreCase(agg.getExpression())
                    && !partitionColRef.equals(agg.getParameters().get(0).getColRef())) {
                return false;
            }
            if (!FunctionDesc.FUNC_MAX.equalsIgnoreCase(agg.getExpression())
                    && CollectionUtils.isNotEmpty(agg.getParameters())) {
                return false;
            }
        }

        return true;
    }

    private PartitionDesc getPartitionDesc(NDataflow dataflow, OlapContext olapContext) {
        NDataModel model = dataflow.getModel();
        boolean isStreamingFactTable = olapContext.getFirstTableScan().getOlapTable().getSourceTable()
                .getSourceType() == ISourceAware.ID_STREAMING;
        boolean isBatchFusionModel = isStreamingFactTable && dataflow.getModel().isFusionModel()
                && !dataflow.isStreaming();
        if (!isBatchFusionModel) {
            return model.getPartitionDesc();
        }
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), dataflow.getProject())
                .getDataModelDesc(model.getFusionId()).getPartitionDesc();
    }

    private Pair<RexNode, RexNode> transformSegment2RexCall(NDataSegment dataSegment, String dateFormat,
            RexBuilder rexBuilder, RexInputRef partitionColInputRef, DataType partitionColType, boolean isStreaming) {
        String start;
        String end;
        if (dataSegment.isOffsetCube()) {
            start = DateFormat.formatToDateStr(dataSegment.getKSRange().getStart(), dateFormat);
            end = DateFormat.formatToDateStr(dataSegment.getKSRange().getEnd(), dateFormat);
        } else {
            Pair<String, String> pair = transformDateType(dataSegment, partitionColType, dateFormat);
            start = pair.getFirst();
            end = pair.getSecond();
        }

        val startRexLiteral = RexUtils.transformValue2RexLiteral(rexBuilder, start, partitionColType);
        val endRexLiteral = RexUtils.transformValue2RexLiteral(rexBuilder, end, partitionColType);
        val greaterThanOrEqualCall = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
                Lists.newArrayList(partitionColInputRef, startRexLiteral));

        // the right side of streaming segments is closed, like [start, end], while batch segment is [start, end)
        val sqlOperator = isStreaming ? SqlStdOperatorTable.LESS_THAN_OR_EQUAL : SqlStdOperatorTable.LESS_THAN;
        val lessCall = rexBuilder.makeCall(sqlOperator, Lists.newArrayList(partitionColInputRef, endRexLiteral));
        return Pair.newPair(greaterThanOrEqualCall, lessCall);
    }

    private Pair<String, String> transformDateType(NDataSegment dataSegment, DataType colType, String dateFormat) {
        long segmentStartTs = dataSegment.getTSRange().getStart();
        long segmentEndTs = dataSegment.getTSRange().getEnd();
        String formattedStart = DateFormat.formatToDateStr(segmentStartTs, dateFormat);
        String formattedEnd = DateFormat.formatToDateStr(segmentEndTs, dateFormat);
        String start = checkAndReformatDateType(formattedStart, segmentStartTs, colType);
        String end = checkAndReformatDateType(formattedEnd, segmentEndTs, colType);
        return Pair.newPair(start, end);
    }

    private static String checkAndReformatDateType(String formattedValue, long segmentTs, DataType colType) {
        switch (colType.getName()) {
        case DataType.DATE:
            if (DATE_PATTERN.matcher(formattedValue).matches()) {
                return formattedValue;
            }
            return DateFormat.formatToDateStr(segmentTs, DateFormat.DEFAULT_DATE_PATTERN);
        case DataType.TIMESTAMP:
            if (TIMESTAMP_PATTERN.matcher(formattedValue).matches()) {
                return formattedValue;
            }
            return DateFormat.formatToDateStr(segmentTs, DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS);
        case DataType.VARCHAR:
        case DataType.STRING:
        case DataType.INTEGER:
        case DataType.BIGINT:
            return formattedValue;
        default:
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "%s data type is not supported for partition column", colType));
        }
    }
}
