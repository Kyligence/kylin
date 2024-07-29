package io.kyligence.kap.engine.spark.job;

import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.engine.spark.job.LogJobInfoUtils;
import org.apache.kylin.engine.spark.job.ResourceDetect;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.spark.sql.hive.utils.ResourceDetectUtils;

public class ResourceDetectBeforeOptimizeJob extends LayoutDataOptimizeJob implements ResourceDetect {

    public static void main(String[] args) {
        ResourceDetectBeforeOptimizeJob resourceDetectJob = new ResourceDetectBeforeOptimizeJob();
        resourceDetectJob.execute(args);
    }

    @Override
    protected void doExecute() throws Exception {
        infos.clearOptimizeLayoutIds();
        Map<String, Long> resourceSize = Maps.newHashMap();
        this.getLayoutDetails().foreach(layoutDetails -> {
            infos.recordOptimizeLayoutIds(layoutDetails.getLayoutId());
            resourceSize.put(layoutDetails.getId(), layoutDetails.getSizeInBytes());
            return null;
        });
        ResourceDetectUtils.write(new Path(config.getJobTmpShareDir(project, jobId),
                dataFlow().getId() + "_" + ResourceDetectUtils.fileName()), resourceSize);
    }

    @Override
    protected String generateInfo() {
        return LogJobInfoUtils.resourceDetectBeforeOptimizeJob();
    }

    @Override
    protected void waiteForResourceSuccess() {
        // do nothing
    }
}
