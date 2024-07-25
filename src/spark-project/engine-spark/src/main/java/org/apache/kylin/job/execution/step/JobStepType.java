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

package org.apache.kylin.job.execution.step;

import static org.apache.kylin.job.execution.stage.StageType.BUILD_DICT;
import static org.apache.kylin.job.execution.stage.StageType.BUILD_LAYER;
import static org.apache.kylin.job.execution.stage.StageType.DELETE_USELESS_LAYOUT_DATA;
import static org.apache.kylin.job.execution.stage.StageType.GATHER_FLAT_TABLE_STATS;
import static org.apache.kylin.job.execution.stage.StageType.GENERATE_FLAT_TABLE;
import static org.apache.kylin.job.execution.stage.StageType.INTERNAL_TABLE_LOAD;
import static org.apache.kylin.job.execution.stage.StageType.MATERIALIZED_FACT_TABLE;
import static org.apache.kylin.job.execution.stage.StageType.MERGE_COLUMN_BYTES;
import static org.apache.kylin.job.execution.stage.StageType.MERGE_FLAT_TABLE;
import static org.apache.kylin.job.execution.stage.StageType.MERGE_INDICES;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_COMPACTION;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_REPARTITION;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_ZORDER;
import static org.apache.kylin.job.execution.stage.StageType.REFRESH_COLUMN_BYTES;
import static org.apache.kylin.job.execution.stage.StageType.REFRESH_SNAPSHOTS;
import static org.apache.kylin.job.execution.stage.StageType.SNAPSHOT_BUILD;
import static org.apache.kylin.job.execution.stage.StageType.TABLE_SAMPLING;
import static org.apache.kylin.job.execution.stage.StageType.WAITE_FOR_RESOURCE;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.DefaultExecutable;
import org.apache.kylin.job.execution.DefaultExecutableOnModel;
import org.apache.kylin.job.execution.NSparkExecutable;
import org.apache.kylin.job.execution.handler.ExecutableHandlerFactory;

import io.kyligence.kap.engine.spark.job.InternalTableLoadingStep;
import io.kyligence.kap.engine.spark.job.NResourceDetectStep;
import io.kyligence.kap.engine.spark.job.NSparkCleanupAfterMergeStep;
import io.kyligence.kap.engine.spark.job.NSparkCubingStep;
import io.kyligence.kap.engine.spark.job.NSparkMergingStep;
import io.kyligence.kap.engine.spark.job.NSparkLayoutDataOptimizeStep;
import io.kyligence.kap.engine.spark.job.NSparkSnapshotBuildingStep;
import io.kyligence.kap.engine.spark.job.NSparkUpdateMetadataStep;
import io.kyligence.kap.engine.spark.job.NTableSamplingJob;
import io.kyligence.kap.engine.spark.job.SparkCleanupTransactionalTableStep;
import lombok.extern.slf4j.Slf4j;
@Slf4j
public enum JobStepType {
    RESOURCE_DETECT {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            if (config.getSparkEngineBuildStepsToSkip().contains(NResourceDetectStep.class.getName())) {
                return null;
            }
            return new NResourceDetectStep(parent);
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_AFTER_MERGE {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            AbstractExecutable step = new NSparkCleanupAfterMergeStep();
            return step;

        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },
    CUBING {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new NSparkCubingStep(config.getSparkBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            REFRESH_SNAPSHOTS.createStage(parent, config);

            MATERIALIZED_FACT_TABLE.createStage(parent, config);
            BUILD_DICT.createStage(parent, config);
            GENERATE_FLAT_TABLE.createStage(parent, config);
            GATHER_FLAT_TABLE_STATS.createStage(parent, config);
            BUILD_LAYER.createStage(parent, config);
            REFRESH_COLUMN_BYTES.createStage(parent, config);
        }
    },
    MERGING {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new NSparkMergingStep(config.getSparkMergeClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);

            MERGE_FLAT_TABLE.createStage(parent, config);
            MERGE_INDICES.createStage(parent, config);
            MERGE_COLUMN_BYTES.createStage(parent, config);
        }
    },

    BUILD_SNAPSHOT {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new NSparkSnapshotBuildingStep(config.getSnapshotBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            SNAPSHOT_BUILD.createStage(parent, config);
        }
    },

    BUILD_INTERNAL {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new InternalTableLoadingStep(config.getInternalTableBuildClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            INTERNAL_TABLE_LOAD.createStage(parent, config);
        }
    },

    SAMPLING {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new NTableSamplingJob.SamplingStep(config.getSparkTableSamplingClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);
            TABLE_SAMPLING.createStage(parent, config);
        }
    },

    UPDATE_METADATA {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            if (!(parent instanceof DefaultExecutableOnModel)) {
                throw new IllegalArgumentException();
            }
            ((DefaultExecutableOnModel) parent).setHandler(
                    ExecutableHandlerFactory.createExecutableHandler((DefaultExecutableOnModel) parent));
            return new NSparkUpdateMetadataStep();
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },

    CLEAN_UP_TRANSACTIONAL_TABLE {
        @Override
        public AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new SparkCleanupTransactionalTableStep();
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
        }
    },
    LAYOUT_DATA_OPTIMIZE {
        @Override
        protected AbstractExecutable create(DefaultExecutable parent, KylinConfig config) {
            return new NSparkLayoutDataOptimizeStep(config.getSparkOptimizeClassName());
        }

        @Override
        protected void addSubStage(NSparkExecutable parent, KylinConfig config) {
            WAITE_FOR_RESOURCE.createStage(parent, config);

            DELETE_USELESS_LAYOUT_DATA.createStage(parent, config);
            OPTIMIZE_LAYOUT_DATA_BY_REPARTITION.createStage(parent, config);
            OPTIMIZE_LAYOUT_DATA_BY_ZORDER.createStage(parent, config);
            OPTIMIZE_LAYOUT_DATA_BY_COMPACTION.createStage(parent, config);
        }
    };

    protected abstract AbstractExecutable create(DefaultExecutable parent, KylinConfig config);

    /** add stage in spark executable */
    protected abstract void addSubStage(NSparkExecutable parent, KylinConfig config);

    public AbstractExecutable createStep(DefaultExecutable parent, KylinConfig config) {
        AbstractExecutable step = create(parent, config);
        if (step == null) {
            log.info("{} skipped", this);
        } else {
            addParam(parent, step);
        }
        return step;
    }

    protected void addParam(DefaultExecutable parent, AbstractExecutable step) {
        step.setParams(parent.getParams());
        step.setProject(parent.getProject());
        step.setTargetSubject(parent.getTargetSubject());
        step.setJobType(parent.getJobType());
        parent.addTask(step);
        if (step instanceof NSparkExecutable) {
            addSubStage((NSparkExecutable) step, KylinConfig.readSystemKylinConfig());
            ((NSparkExecutable) step).setStageMap();

            ((NSparkExecutable) step).setDistMetaUrl(
                    KylinConfig.readSystemKylinConfig().getJobTmpMetaStoreUrl(parent.getProject(), step.getId()));
        }
        if (CollectionUtils.isNotEmpty(parent.getTargetPartitions())) {
            step.setTargetPartitions(parent.getTargetPartitions());
        }
    }

}
