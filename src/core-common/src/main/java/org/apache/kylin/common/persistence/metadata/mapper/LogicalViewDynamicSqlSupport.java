package org.apache.kylin.common.persistence.metadata.mapper;

public final class LogicalViewDynamicSqlSupport {

    public static final LogicalView sqlTable = new LogicalView();

    private LogicalViewDynamicSqlSupport() {
    }

    public static final class LogicalView extends BasicSqlTable<LogicalView> {

        public LogicalView() {
            super("logical_view", LogicalView::new);
        }
    }
}
