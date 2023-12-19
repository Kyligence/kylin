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

package org.apache.kylin.planner;

import java.util.Map;
import java.util.Optional;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataflow;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.planner.execution.PlannerExecution;
import org.apache.kylin.planner.internal.PlannerConf;
import org.apache.kylin.planner.internal.PlannerState;
import org.apache.kylin.planner.plans.CubePlan;
import org.apache.kylin.planner.plans.PlanOptimizer;
import org.apache.kylin.planner.plans.PlanParser;

public class CubePlanner {

    private final CubePlan plan;
    private final PlannerState state;

    private CubePlanner(CubePlan plan, PlannerState state) {
        this.plan = plan;
        this.state = state;
    }

    public static Builder builder() {
        return new Builder();
    }

    public CubePlan plan() {
        return plan;
    }

    public PlannerState plannerState() {
        return state;
    }

    public CubePlan optimizeCube() {
        PlannerExecution pe = new PlannerExecution(this, plan);
        return pe.optimizePlan();
    }

    public static class Builder {

        private KylinConfig config;
        private NDataModel model;
        private IndexPlan indexPlan;
        private NDataflow dataflow;
        private Map<String, String> params;

        public Builder config(KylinConfig config) {
            this.config = config;
            return this;
        }

        public Builder model(NDataModel model) {
            this.model = model;
            return this;
        }

        public Builder indexPlan(IndexPlan indexPlan) {
            this.indexPlan = indexPlan;
            return this;
        }

        public Builder dataflow(NDataflow dataflow) {
            this.dataflow = dataflow;
            return this;
        }

        public Builder params(Map<String, String> params) {
            this.params = params;
            return this;
        }

        public CubePlanner build() {
            // conf
            final PlannerConf conf = PlannerConf.get();
            Optional.ofNullable(config).ifPresent(kylinConf -> PlannerConf.mergeKylinConf(conf, kylinConf));
            Optional.ofNullable(params).ifPresent(confMap -> PlannerConf.mergeParamConfigs(conf, confMap));
            // optimizer
            final PlanOptimizer optimizer = new PlanOptimizer();
            // parser, plan
            final PlanParser parser = new PlanParser();
            final CubePlan plan = parser.parsePlan(model, indexPlan, dataflow);
            return new CubePlanner(plan, new PlannerState(conf, parser, optimizer));
        }
    }
}
