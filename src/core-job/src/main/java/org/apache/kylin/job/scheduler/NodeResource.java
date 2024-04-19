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

package org.apache.kylin.job.scheduler;

import org.apache.kylin.job.core.AbstractJobExecutable;

public class NodeResource {

    private final AbstractJobExecutable jobExecutable;

    private final int memory;

    public NodeResource(AbstractJobExecutable jobExecutable) {
        this.jobExecutable = jobExecutable;
        this.memory = evaluateMemory(jobExecutable);
    }

    @Override
    public String toString() {
        return "NodeResource{" //
                + "project=" + jobExecutable.getProject() //
                + ", job=" + jobExecutable.getJobId() //
                + ", memory=" + memory + "MB" //
                + '}';
    }

    public int getMemory() {
        return memory;
    }

    private int evaluateMemory(AbstractJobExecutable jobExecutable) {
        return jobExecutable.computeStepDriverMemory();
    }
}
