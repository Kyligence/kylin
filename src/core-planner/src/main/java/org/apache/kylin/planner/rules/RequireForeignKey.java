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

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.planner.plans.CubePlan;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RequireForeignKey extends Rule<CubePlan> {

    @Override
    public CubePlan apply(CubePlan plan) {
        if (plan.cuboidList().isEmpty()) {
            List<String> nameList = plan.getModel().getJoinTables().stream()
                    .map(j -> j.getJoin().getForeignKeyColumns()).flatMap(Arrays::stream)
                    .map(TblColRef::getAliasDotName).collect(Collectors.toList());
            BitSet bits = plan.getColumnBits(nameList);
            if (log.isInfoEnabled()) {
                log.info("Require foreign key cuboid column: {}", plan.getColumnList(bits).stream()
                        .map(TblColRef::getAliasDotName).collect(Collectors.joining(",", "[", "]")));
            }

            List<NDataModel.Measure> measureList = plan.getMeasureList();
            plan.addMergeSetMandatory(bits, measureList);
        }

        return plan;
    }

}
