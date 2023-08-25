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

package io.kyligence.kap.query.optrule;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlSplittableAggFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.kylin.query.relnode.KapAggregateRel;
import org.apache.kylin.query.relnode.KapJoinRel;
import org.apache.kylin.query.relnode.KapNonEquiJoinRel;
import org.apache.kylin.query.relnode.KapProjectRel;
import org.apache.kylin.query.relnode.KapValuesRel;

public class ScalarSubqueryJoinRule extends RelOptRule {

    // JOIN_PREDICATE from guava's Predicate,

    public static final ScalarSubqueryJoinRule AGG_JOIN = new ScalarSubqueryJoinRule(//
            operand(KapAggregateRel.class, //
                    operand(Join.class, //
                            null, j -> j instanceof KapJoinRel || j instanceof KapNonEquiJoinRel, any())),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_JOIN");

    public static final ScalarSubqueryJoinRule AGG_PRJ_JOIN = new ScalarSubqueryJoinRule(//
            operand(KapAggregateRel.class, //
                    operand(KapProjectRel.class, //
                            operand(Join.class, //
                                    null, j -> j instanceof KapJoinRel || j instanceof KapNonEquiJoinRel, any()))),
            RelFactories.LOGICAL_BUILDER, "ScalarSubqueryJoinRule:AGG_PRJ_JOIN");

    public ScalarSubqueryJoinRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Join join = call.rel(call.rels.length - 1);
        switch (join.getJoinType()) {
        case INNER:
        case LEFT:
            break;
        default:
            return false;
        }

        KapAggregateRel aggregate = call.rel(0);
        if (!aggregate.isSimpleGroupType() || aggregate.getAggCallList().isEmpty()) {
            return false;
        }

        // If any aggregate functions do not support splitting, bail outer
        // If any aggregate call has a filter or is distinct, bail out.
        // Count-distinct is currently not supported,
        //  but this can be achieved in case of scalar-subquery with distinct values.
        return aggregate.getAggCallList().stream().noneMatch(a -> a.hasFilter() //
                || a.isDistinct() //
                || Objects.isNull(a.getAggregation().unwrap(SqlSplittableAggFunction.class)));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Transposer transposer = new Transposer(call);
        if (!transposer.isTransposable()) {
            return;
        }

        RelNode relNode = transposer.getTransposedRel();
        call.transformTo(relNode);
    }

    private <E> SqlSplittableAggFunction.Registry<E> createRegistry(final List<E> list) {
        return e -> {
            int i = list.indexOf(e);
            if (i < 0) {
                i = list.size();
                list.add(e);
            }
            return i;
        };
    }

    private class Transposer {
        // base variables
        private final RelOptRuleCall call;
        private final AggregateElem aggregate;
        private final Join join;

        // join sides
        private final LeftSide left;
        private final RightSide right;

        // maintainer
        public Transposer(RelOptRuleCall ruleCall) {
            // call -> "{agg, join}"
            call = ruleCall;
            aggregate = createAggregateElem(ruleCall);
            join = ruleCall.rel(ruleCall.rels.length - 1);

            // join sides
            RelMetadataQuery mq = call.getMetadataQuery();
            ImmutableBitSet joinCondSet = RelOptUtil.InputFinder.bits(join.getCondition());
            ImmutableBitSet aggJoinSet = aggregate.getGroupSet().union(joinCondSet);
            left = new LeftSide(join.getLeft(), join.getInput(0), mq, aggJoinSet);
            right = new RightSide(left, join.getRight(), join.getInput(1), mq, aggJoinSet);
        }

        public boolean isTransposable() {
            return left.isAggregable() || right.isAggregable();
        }

        public RelNode getTransposedRel() {
            // builders
            final RelBuilder relBuilder = call.builder();
            final RexBuilder rexBuilder = aggregate.getRexBuilder();

            // modify aggregate
            left.modifyAggregate(aggregate, relBuilder, rexBuilder);
            right.modifyAggregate(aggregate, relBuilder, rexBuilder);

            // agg-join mapping
            final Mapping aggJoinMapping = getAggJoinMapping();

            // create new join
            final RexNode newCondition = RexUtil.apply(aggJoinMapping, join.getCondition());
            relBuilder.push(left.getNewInput()).push(right.getNewInput()).join(join.getJoinType(), newCondition);

            // aggregate above to sum up the sub-totals
            final List<RexNode> projectList = getProjectList(relBuilder, rexBuilder, aggJoinMapping);
            final List<AggregateCall> aggCallList = Lists.newArrayList();
            aggregateAbove(projectList, aggCallList, relBuilder, rexBuilder);

            // create new project
            relBuilder.project(projectList);

            // above aggregate
            // Maybe we can convert aggregate into projects when inner-join.
            final RelBuilder.GroupKey groupKey = relBuilder.groupKey(//
                    Mappings.apply(aggJoinMapping, aggregate.getGroupSet()), //
                    Mappings.apply2(aggJoinMapping, aggregate.getGroupSets()));
            relBuilder.aggregate(groupKey, aggCallList);

            return relBuilder.build();
        }

        private List<RexNode> getProjectList(final RelBuilder relBuilder, // 
                final RexBuilder rexBuilder, //
                final Mapping mapping) {
            // MappingType.INVERSE_SURJECTION
            final List<RexNode> projectList = //
                    Lists.newArrayList(rexBuilder.identityProjects(relBuilder.peek().getRowType()));
            final List<Integer> groupList = //
                    aggregate.getGroupList().stream().map(mapping::getTarget).collect(Collectors.toList());

            // find an more graceful implementation
            // [i0, i1, i2, i3] -> [i1, i0, i2, i3]
            final Map<Integer, RexNode> switchMap = Maps.newHashMap();
            Ord.zip(projectList).forEach(o -> {
                int i = groupList.indexOf(o.i);
                if (i < 0) {
                    return;
                }
                switchMap.put(i, projectList.get(i));
                projectList.set(i, switchMap.getOrDefault(o.i, o.e));
            });

            return projectList;
        }

        private AggregateElem createAggregateElem(RelOptRuleCall call) {
            if (call.rels.length > 2) {
                return new AggregateProject(call.rel(0), call.rel(1));
            }
            return new AggregateElem(call.rel(0));
        }

        private void aggregateAbove(final List<RexNode> projectList, //
                final List<AggregateCall> aggCallList, //
                final RelBuilder relBuilder, //
                final RexBuilder rexBuilder) {
            final int newLeftWidth = left.getNewInputFieldCount();
            final int groupIndicatorCount = aggregate.getGroupIndicatorCount();
            final SqlSplittableAggFunction.Registry<RexNode> projectRegistry = createRegistry(projectList);
            Ord.zip(aggregate.getAggCallList()).forEach(aggCallOrd -> {
                // No need to care about args' mapping.
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                Integer lst = left.getAggOrdinal(aggCallOrd.i);
                Integer rst = right.getAggOrdinal(aggCallOrd.i);
                final AggregateCall newAggCall = //
                        splitAggFunc.topSplit(rexBuilder, projectRegistry, groupIndicatorCount, //
                                relBuilder.peek().getRowType(), aggCall, //
                                Objects.isNull(lst) ? -1 : lst, //
                                Objects.isNull(rst) ? -1 : rst + newLeftWidth);

                if (aggCall.getAggregation() == SqlStdOperatorTable.COUNT //
                        && newAggCall.getAggregation() == SqlStdOperatorTable.SUM0) {
                    aboveCountSum0(rexBuilder, aggCall, newAggCall, projectList);
                }

                aggCallList.add(newAggCall);
            });
        }

        private void aboveCountSum0(final RexBuilder rexBuilder, //
                AggregateCall aggCall, //
                AggregateCall newAggCall, //
                final List<RexNode> projectList) {
            // COUNT(*), COUNT(1), COUNT(COL), COUNT(COL1, COL2, ...)
            final boolean nullAsOne = aggCall.getArgList().isEmpty();
            newAggCall.getArgList().forEach(i -> {
                // old project node
                RexNode p = projectList.get(i);
                // when-then-else
                List<RexNode> wte = Lists.newLinkedList();
                wte.add(rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, p));
                if (nullAsOne) {
                    wte.add(rexBuilder.makeLiteral(BigDecimal.ONE, p.getType(), true));
                } else {
                    wte.add(rexBuilder.makeZeroLiteral(p.getType()));
                }

                wte.add(p);
                // new project node
                RexNode np = rexBuilder.makeCall(p.getType(), SqlStdOperatorTable.CASE, wte);
                projectList.set(i, np);
            });
        }

        private Mapping getAggJoinMapping() {
            final Map<Integer, Integer> map = Maps.newHashMap();
            map.putAll(left.getAggJoinMap());
            map.putAll(right.getAggJoinMap());
            final int sourceCount = join.getRowType().getFieldCount();
            final int targetCount = left.getNewInputFieldCount() + right.getNewInputFieldCount();
            return (Mapping) Mappings.target(map::get, sourceCount, targetCount);
        }

    } // end of Transposer

    private static class AggregateElem {
        // base variables
        protected final KapAggregateRel aggregate;

        // immediate variables
        private RexBuilder rexBuilder;

        public AggregateElem(KapAggregateRel aggregate) {
            this.aggregate = aggregate;
        }

        public RexBuilder getRexBuilder() {
            if (Objects.isNull(rexBuilder)) {
                rexBuilder = aggregate.getCluster().getRexBuilder();
            }
            return rexBuilder;
        }

        public ImmutableBitSet getGroupSet() {
            return aggregate.getGroupSet();
        }

        public ImmutableList<ImmutableBitSet> getGroupSets() {
            return ImmutableList.<ImmutableBitSet> builder().addAll(aggregate.groupSets).build();
        }

        public int getGroupCount() {
            return aggregate.getGroupCount();
        }

        public int getGroupIndicatorCount() {
            return getGroupCount() + aggregate.getIndicatorCount();
        }

        public List<AggregateCall> getAggCallList() {
            return aggregate.getAggCallList();
        }

        public List<Integer> getGroupList() {
            return aggregate.getGroupSet().asList();
        }

    } // end of AggregateElem

    private static class AggregateProject extends AggregateElem {
        // base variables
        private final KapProjectRel project;
        private final Mappings.TargetMapping targetMapping;

        // immediate variables
        private ImmutableBitSet groupSet;
        private ImmutableList<ImmutableBitSet> groupSets;
        private List<AggregateCall> aggCallList;

        public AggregateProject(KapAggregateRel aggregate, KapProjectRel project) {
            super(aggregate);
            this.project = project;
            this.targetMapping = createTargetMapping();
        }

        @Override
        public ImmutableBitSet getGroupSet() {
            if (Objects.isNull(groupSet)) {
                groupSet = Mappings.apply((Mapping) targetMapping, aggregate.getGroupSet());
            }
            return groupSet;
        }

        @Override
        public ImmutableList<ImmutableBitSet> getGroupSets() {
            if (Objects.isNull(groupSets)) {
                groupSets = ImmutableList.<ImmutableBitSet> builder() //
                        .addAll(Mappings.apply2((Mapping) targetMapping, aggregate.getGroupSets())) //
                        .build();
            }
            return groupSets;
        }

        @Override
        public List<AggregateCall> getAggCallList() {
            if (Objects.isNull(aggCallList)) {
                aggCallList = aggregate.getAggCallList().stream() //
                        .map(a -> a.transform(targetMapping)) //
                        .collect(Collectors.collectingAndThen(Collectors.toList(), //
                                Collections::unmodifiableList));
            }
            return aggCallList;
        }

        @Override
        public List<Integer> getGroupList() {
            return super.getGroupList().stream().map(targetMapping::getTarget).collect(Collectors.toList());
        }

        private Mappings.TargetMapping createTargetMapping() {
            if (Objects.isNull(project.getMapping())) {
                return Mappings.createIdentity(project.getRowType().getFieldCount());
            }
            return project.getMapping().inverse();
        }

    } // end of AggregateProject

    private abstract class JoinSide {
        // base variables
        private final RelNode input;
        private final ImmutableBitSet aggJoinSet;

        // util variables
        protected ImmutableBitSet fieldSet;
        protected ImmutableBitSet sideAggJoinSet;
        protected ImmutableBitSet belowAggGroupSet;

        // immediate variables
        private Map<Integer, Integer> aggOrdinalMap;
        private List<AggregateCall> belowAggCallList;
        private SqlSplittableAggFunction.Registry<AggregateCall> belowAggCallRegistry;

        private RelNode newInput;

        private Map<Integer, Integer> aggJoinMap;

        public JoinSide(RelNode input, ImmutableBitSet aggJoinSet) {
            this.input = input;
            this.aggJoinSet = aggJoinSet;
        }

        public abstract boolean isAggregable();

        public RelNode getNewInput() {
            return newInput;
        }

        public Integer getAggOrdinal(int i) {
            return getAggOrdinalMap().get(i);
        }

        public Map<Integer, Integer> getAggJoinMap() {
            if (Objects.isNull(aggJoinMap)) {
                final int belowOffset = getBelowOffset();
                final Map<Integer, Integer> map = Maps.newHashMap();
                Ord.zip(getSideAggJoinSet()).forEach(o -> map.put(o.e, belowOffset + o.i));
                aggJoinMap = map;
            }
            return aggJoinMap;
        }

        public void modifyAggregate(AggregateElem aggregate, RelBuilder relBuilder, RexBuilder rexBuilder) {
            if (isAggregable()) {
                newInput = convertSplit(aggregate, relBuilder, rexBuilder);
                return;
            }
            newInput = convertSingleton(aggregate, relBuilder, rexBuilder);
        }

        protected final boolean isAggregable(RelNode relNode, RelNode input, RelMetadataQuery mq) {
            if (isRelValues(relNode)) {
                return false;
            }

            // No need to do aggregation. There is nothing to be gained by this rule.
            Boolean unique = mq.areColumnsUnique(input, getBelowAggGroupSet());
            return Objects.isNull(unique) || !unique;
        }

        protected int getInputFieldCount() {
            return input.getRowType().getFieldCount();
        }

        protected int getNewInputFieldCount() {
            return Preconditions.checkNotNull(newInput).getRowType().getFieldCount();
        }

        protected abstract int getOffset();

        protected abstract int getBelowOffset();

        protected abstract Mappings.TargetMapping getTargetMapping();

        private Map<Integer, Integer> getAggOrdinalMap() {
            if (Objects.isNull(aggOrdinalMap)) {
                aggOrdinalMap = Maps.newHashMap();
            }
            return aggOrdinalMap;
        }

        private void registryAggCall(int i, int offset, AggregateCall aggCall) {
            getAggOrdinalMap().put(i, offset + registry(aggCall));
        }

        private void registryOther(int i, int ordinal) {
            getAggOrdinalMap().put(i, ordinal);
        }

        private ImmutableBitSet getFieldSet() {
            if (Objects.isNull(fieldSet)) {
                int offset = getOffset();
                fieldSet = ImmutableBitSet.range(offset, offset + getInputFieldCount());
            }
            return fieldSet;
        }

        private ImmutableBitSet getBelowAggGroupSet() {
            if (Objects.isNull(belowAggGroupSet)) {
                int offset = getOffset();
                belowAggGroupSet = getSideAggJoinSet().shift(-offset);
            }
            return belowAggGroupSet;
        }

        private ImmutableBitSet getSideAggJoinSet() {
            if (Objects.isNull(sideAggJoinSet)) {
                ImmutableBitSet fieldSet0 = getFieldSet();
                sideAggJoinSet = Preconditions.checkNotNull(aggJoinSet).intersect(fieldSet0);
            }
            return sideAggJoinSet;
        }

        private RelNode convertSplit(AggregateElem aggregate, RelBuilder relBuilder, RexBuilder rexBuilder) {
            final ImmutableBitSet fields = getFieldSet();
            final int oldGroupSetCount = aggregate.getGroupCount();
            final int newGroupSetCount = getBelowAggGroupSet().cardinality();
            Ord.zip(aggregate.getAggCallList()).forEach(aggCallOrd -> {
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                ImmutableBitSet aggArgSet = ImmutableBitSet.of(aggCall.getArgList());
                final AggregateCall newAggCall;
                if (fields.contains(aggArgSet)) {
                    // convert split
                    AggregateCall splitAggCall = splitAggFunc.split(aggCall, getTargetMapping());
                    newAggCall = splitAggCall.adaptTo(input, splitAggCall.getArgList(), splitAggCall.filterArg, //
                            oldGroupSetCount, newGroupSetCount);
                } else {
                    newAggCall = splitOther(splitAggFunc, rexBuilder, aggCall, fields, aggArgSet);
                }

                if (Objects.isNull(newAggCall)) {
                    return;
                }
                registryAggCall(aggCallOrd.i, newGroupSetCount, newAggCall);
            });

            return relBuilder.push(input) //
                    .aggregate(relBuilder.groupKey(belowAggGroupSet, null), //
                            Preconditions.checkNotNull(belowAggCallList)) //
                    .build();
        }

        private AggregateCall splitOther(SqlSplittableAggFunction splitAggFunc, //
                RexBuilder rexBuilder, //
                AggregateCall aggCall, //
                ImmutableBitSet fields, //
                ImmutableBitSet args) {
            // Thinking...aggCall not transformed?
            AggregateCall other = splitAggFunc.other(rexBuilder.getTypeFactory(), aggCall);
            if (Objects.isNull(other)) {
                return null;
            }

            ImmutableBitSet newArgSet = Mappings.apply((Mapping) getTargetMapping(), args.intersect(fields));
            return AggregateCall.create(other.getAggregation(), other.isDistinct(), //
                    other.isApproximate(), newArgSet.asList(), //
                    other.filterArg, other.getType(), other.getName());

        }

        private RelNode convertSingleton(AggregateElem aggregate, RelBuilder relBuilder, RexBuilder rexBuilder) {
            relBuilder.push(input);
            final ImmutableBitSet fieldSet0 = getFieldSet();
            final List<RexNode> projectList = Lists.newArrayList();
            getBelowAggGroupSet().forEach(i -> projectList.add(relBuilder.field(i)));
            Ord.zip(aggregate.getAggCallList()).forEach(aggCallOrd -> {
                AggregateCall aggCall = aggCallOrd.e;
                SqlAggFunction aggFunc = aggCall.getAggregation();
                SqlSplittableAggFunction splitAggFunc = Preconditions
                        .checkNotNull(aggFunc.unwrap(SqlSplittableAggFunction.class));
                if (aggCall.getArgList().isEmpty()) {
                    return;
                }
                ImmutableBitSet aggArgSet = ImmutableBitSet.of(aggCall.getArgList());
                if (!fieldSet0.contains(aggArgSet)) {
                    return;
                }

                // convert singleton
                RexNode singleton = splitAggFunc.singleton(rexBuilder, input.getRowType(), //
                        aggCall.transform(getTargetMapping()));
                if (singleton instanceof RexInputRef) {
                    registryOther(aggCallOrd.i, ((RexInputRef) singleton).getIndex());
                    return;
                }
                int ordinal = projectList.size();
                projectList.add(singleton);
                registryOther(aggCallOrd.i, ordinal);
            });

            relBuilder.project(projectList);

            return relBuilder.build();
        }

        private boolean isRelValues(RelNode node) {
            if (node instanceof HepRelVertex) {
                RelNode current = ((HepRelVertex) node).getCurrentRel();
                if (current instanceof KapValuesRel) {
                    return true;
                }

                if (current.getInputs().isEmpty()) {
                    return false;
                }

                return current.getInputs().stream().allMatch(this::isRelValues);
            }
            return false;
        }

        private int registry(AggregateCall aggCall) {
            if (Objects.isNull(belowAggCallRegistry)) {
                if (Objects.isNull(belowAggCallList)) {
                    belowAggCallList = Lists.newArrayList();
                }
                belowAggCallRegistry = createRegistry(belowAggCallList);
            }
            return belowAggCallRegistry.register(aggCall);
        }

    } // end of side

    private class LeftSide extends JoinSide {

        private final boolean isAggregable;

        private final Mappings.TargetMapping targetMapping;

        public LeftSide(RelNode relNode, RelNode input, RelMetadataQuery mq, ImmutableBitSet aggJoinSet) {
            super(input, aggJoinSet);
            this.isAggregable = isAggregable(relNode, input, mq);
            this.targetMapping = createTargetMapping();
        }

        @Override
        public boolean isAggregable() {
            return isAggregable;
        }

        @Override
        protected int getOffset() {
            return 0;
        }

        @Override
        protected int getBelowOffset() {
            return 0;
        }

        @Override
        protected Mappings.TargetMapping getTargetMapping() {
            return targetMapping;
        }

        private Mappings.TargetMapping createTargetMapping() {
            int fieldCount = getInputFieldCount();
            return Mappings.createIdentity(fieldCount);
        }

    } // end of LeftSide

    private class RightSide extends JoinSide {

        private final LeftSide left;

        private final boolean isAggregable;

        private final Mappings.TargetMapping targetMapping;

        public RightSide(LeftSide left, //
                RelNode relNode, RelNode input, //
                RelMetadataQuery mq, ImmutableBitSet aggJoinSet) {
            super(input, aggJoinSet);
            this.left = left;
            this.isAggregable = isAggregable(relNode, input, mq);
            this.targetMapping = createTargetMapping();
        }

        @Override
        public boolean isAggregable() {
            return isAggregable;
        }

        @Override
        protected int getOffset() {
            return left.getInputFieldCount();
        }

        @Override
        protected int getBelowOffset() {
            return left.getNewInputFieldCount();
        }

        @Override
        protected Mappings.TargetMapping getTargetMapping() {
            return targetMapping;
        }

        private Mappings.TargetMapping createTargetMapping() {
            int offset = getOffset();
            int fieldCount = getInputFieldCount();
            return Mappings.createShiftMapping(fieldCount + offset, 0, offset, fieldCount);
        }

    } // end of RightSide

}
