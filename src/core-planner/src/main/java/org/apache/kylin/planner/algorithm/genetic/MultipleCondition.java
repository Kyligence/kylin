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

import java.util.List;

import org.apache.commons.math3.genetics.Population;
import org.apache.commons.math3.genetics.StoppingCondition;
import org.apache.kylin.guava30.shaded.common.collect.ImmutableList;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MultipleCondition implements StoppingCondition {

    private final List<StoppingCondition> conditionList;

    public MultipleCondition(StoppingCondition one, StoppingCondition... more) {
        this.conditionList = ImmutableList.<StoppingCondition> builder().add(one).add(more).build();
    }

    @Override
    public boolean isSatisfied(Population pop) {
        for (StoppingCondition cond : conditionList) {
            if (cond.isSatisfied(pop)) {
                log.info("Stopping condition satisfied: {}", cond);
                return true;
            }
        }
        return false;
    }
}
