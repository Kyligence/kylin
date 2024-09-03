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

package org.apache.kylin.metadata.cube.cuboid;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.IRealizationCandidate;

import lombok.Getter;

/**
 * Both Snapshot and InternalTable can be used as LookupCandidate.
 */
@Getter
public class NLookupCandidate implements IRealizationCandidate {

    private final String table;
    private final Type type;

    public NLookupCandidate(String table, Type type) {
        this.table = table;
        this.type = type;
    }

    @Override
    public double getCost() {
        return 0d;
    }

    public enum Type {
        SNAPSHOT,

        INTERNAL_TABLE,

        NONE
    }

    public static Type getType(KylinConfig config) {
        return config.isInternalTableEnabled() ? Type.INTERNAL_TABLE : Type.SNAPSHOT;
    }
}
