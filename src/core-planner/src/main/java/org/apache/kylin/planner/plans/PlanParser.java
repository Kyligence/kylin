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

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.planner.internal.PlannerConfHelper;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PlanParser implements PlannerConfHelper {

    public CubePlan parsePlan(NDataModel model, IndexPlan indexPlan, NDataflow dataflow) {
        CubePlan plan = new CubePlan(model, indexPlan, dataflow);
        plan.analyze();
        return plan;
    }
}
