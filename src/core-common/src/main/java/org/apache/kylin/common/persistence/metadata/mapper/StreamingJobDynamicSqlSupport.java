package org.apache.kylin.common.persistence.metadata.mapper;

public final class StreamingJobDynamicSqlSupport {

    public static final StreamingJob sqlTable = new StreamingJob();

    private StreamingJobDynamicSqlSupport() {
    }

    public static final class StreamingJob extends BasicSqlTable<StreamingJob> {
        public StreamingJob() {
            super("streaming_job", StreamingJob::new);
        }
    }
}
