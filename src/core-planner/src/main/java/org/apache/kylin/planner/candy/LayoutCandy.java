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

package org.apache.kylin.planner.candy;

import java.util.Collections;
import java.util.List;

public class LayoutCandy {

    private final List<Integer> dimensionList;
    private final List<Integer> measureList;
    private final List<Integer> sortBy;
    private final List<Integer> shardBy;

    public LayoutCandy(List<Integer> dimensionList, List<Integer> measureList) {
        this(dimensionList, measureList, Collections.emptyList(), Collections.emptyList());
    }

    public LayoutCandy(List<Integer> dimensionList, List<Integer> measureList, List<Integer> sortBy,
            List<Integer> shardBy) {
        this.dimensionList = dimensionList;
        this.measureList = measureList;
        this.sortBy = sortBy;
        this.shardBy = shardBy;
    }

    public List<Integer> getDimensionList() {
        return dimensionList;
    }

    public List<Integer> getMeasureList() {
        return measureList;
    }

    public List<Integer> getSortBy() {
        return sortBy;
    }

    public List<Integer> getShardBy() {
        return shardBy;
    }
}
