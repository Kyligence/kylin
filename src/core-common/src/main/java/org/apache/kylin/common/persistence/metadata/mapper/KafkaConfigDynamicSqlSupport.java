package org.apache.kylin.common.persistence.metadata.mapper;

import java.sql.JDBCType;

import org.mybatis.dynamic.sql.SqlColumn;

public final class KafkaConfigDynamicSqlSupport {

    public static final KafkaConfig sqlTable = new KafkaConfig();

    private KafkaConfigDynamicSqlSupport() {
    }

    public static final class KafkaConfig extends BasicSqlTable<KafkaConfig> {
        public final SqlColumn<String> db = column("db", JDBCType.VARCHAR);

        public final SqlColumn<String> name = column("name", JDBCType.VARCHAR);
        public KafkaConfig() {
            super("kafka_config", KafkaConfig::new);
        }
    }
}
