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

package org.apache.kylin.planner.internal;

import org.apache.kylin.planner.plans.PlanOptimizer;
import org.apache.kylin.planner.plans.PlanParser;

public class PlannerState {

    private final PlannerConf conf;
    private final PlanParser parser;
    private final PlanOptimizer optimizer;

    public PlannerState(PlannerConf conf, PlanParser parser, PlanOptimizer optimizer) {
        this.conf = conf;
        this.parser = parser;
        this.optimizer = optimizer;
    }

    public PlannerConf getConf() {
        return conf;
    }

    public PlanParser getParser() {
        return parser;
    }

    public PlanOptimizer getOptimizer() {
        return optimizer;
    }
}
