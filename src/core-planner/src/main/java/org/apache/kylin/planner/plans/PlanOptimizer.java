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

import java.util.Arrays;
import java.util.List;

import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.planner.rules.ExcludeColumn;
import org.apache.kylin.planner.rules.ExcludeTable;
import org.apache.kylin.planner.rules.HighCardinalityCollision;
import org.apache.kylin.planner.rules.LayerClustering;
import org.apache.kylin.planner.rules.RemoveDataType;
import org.apache.kylin.planner.rules.RequireForeignKey;
import org.apache.kylin.planner.rules.RequirePartitionColumn;
import org.apache.kylin.planner.rules.RequireSingleDimension;
import org.apache.kylin.planner.rules.RuleBatch;
import org.apache.kylin.planner.rules.RuleExecutor;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PlanOptimizer extends RuleExecutor<CubePlan> {

    private final List<RuleBatch<CubePlan>> preList = Lists.newLinkedList();
    private final List<RuleBatch<CubePlan>> postList = Lists.newLinkedList();

    @SafeVarargs
    public final void offerHead(RuleBatch<CubePlan>... batches) {
        preList.addAll(Arrays.asList(batches));
    }

    @SafeVarargs
    public final void offerTail(RuleBatch<CubePlan>... batches) {
        postList.addAll(Arrays.asList(batches));
    }

    @Override
    protected final List<RuleBatch<CubePlan>> batchList() {
        return ImmutableList.<RuleBatch<CubePlan>> builder().addAll(preList).addAll(defaultList()).addAll(postList)
                .build();
    }

    protected final List<RuleBatch<CubePlan>> defaultList() {
        return Lists.newArrayList(new RuleBatch<>("Common rule set",
                Lists.newArrayList(new RequireSingleDimension(), new RequireForeignKey(), new RequirePartitionColumn(),
                        new ExcludeTable(), new ExcludeColumn(), new RemoveDataType(),
                        new HighCardinalityCollision(), new LayerClustering())));
    }

}
