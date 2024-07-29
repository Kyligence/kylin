package io.kyligence.kap.engine.spark.job;

import static org.apache.kylin.job.execution.stage.StageType.DELETE_USELESS_LAYOUT_DATA;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_COMPACTION;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_REPARTITION;
import static org.apache.kylin.job.execution.stage.StageType.OPTIMIZE_LAYOUT_DATA_BY_ZORDER;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.engine.spark.job.AbstractLayoutDataOptimizeJob;
import org.apache.kylin.engine.spark.job.exec.BuildExec;
import org.apache.kylin.engine.spark.job.exec.OptimizeExec;
import org.apache.kylin.engine.spark.job.stage.BuildParam;

import lombok.val;

public class LayoutDataOptimizeJob extends AbstractLayoutDataOptimizeJob {

    public static void main(String[] args) {
        LayoutDataOptimizeJob layoutDataOptimizeJob = new LayoutDataOptimizeJob();
        layoutDataOptimizeJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        val jobStepId = StringUtils.replace(infos.getJobStepId(), JOB_NAME_PREFIX, "");
        val exec = new OptimizeExec(jobStepId);

        val buildParam = new BuildParam();
        DELETE_USELESS_LAYOUT_DATA.createStage(this, null, buildParam, exec);
        OPTIMIZE_LAYOUT_DATA_BY_REPARTITION.createStage(this, null, buildParam, exec);
        OPTIMIZE_LAYOUT_DATA_BY_ZORDER.createStage(this, null, buildParam, exec);
        OPTIMIZE_LAYOUT_DATA_BY_COMPACTION.createStage(this, null, buildParam, exec);

        exec.optimize();
    }
}
