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

import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.planner.plans.CubePlan;
import org.apache.kylin.planner.util.BitUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequirePartitionColumn extends Rule<CubePlan> {

    @Override
    public CubePlan apply(CubePlan plan) {
        PartitionDesc desc = plan.getModel().getPartitionDesc();
        if (!PartitionDesc.isEmptyPartitionDesc(desc)) {
            final BitSet addBits = plan
                    .getColumnBits(Lists.newArrayList(desc.getPartitionDateColumnRef().getAliasDotName()));

            if (log.isInfoEnabled()) {
                log.info("Require partition cuboid column: {}", plan.getColumnList(addBits).stream()
                        .map(TblColRef::getAliasDotName).collect(Collectors.joining(",", "[", "]")));
            }

            for (Map.Entry<BitSet, List<NDataModel.Measure>> entry : plan.nonHitCuboidMeasureMap().entrySet()) {
                BitSet bits = BitUtil.cloneBits(entry.getKey());
                bits.or(addBits);
                if (!bits.equals(entry.getKey())) {
                    plan.remove(entry.getKey());
                    plan.addMerge(bits, entry.getValue());
                }
            }
        }

        return plan;
    }

}
