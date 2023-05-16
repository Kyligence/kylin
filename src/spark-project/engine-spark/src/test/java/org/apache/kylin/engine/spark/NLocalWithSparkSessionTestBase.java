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
package org.apache.kylin.engine.spark;

import java.io.File;
import java.io.Serializable;
import java.net.BindException;
import java.util.Locale;

import org.apache.calcite.util.CancelFlag;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.util.RandomUtil;
import org.apache.kylin.common.util.TempMetadataBuilder;
import org.apache.kylin.engine.spark.utils.SparkJobFactoryUtils;
import org.apache.kylin.guava30.shaded.common.base.Preconditions;
import org.apache.kylin.job.util.JobContextUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.NTableMetadataManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.optimizer.ConvertInnerJoinToSemiJoin;
import org.apache.spark.sql.internal.StaticSQLConf;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class NLocalWithSparkSessionTestBase extends NLocalFileMetadataTestCase implements Serializable {

    private static final String CSV_TABLE_DIR = TempMetadataBuilder.TEMP_TEST_METADATA + "/data/%s.csv";
    private static final String GLUTEN_CH_LIB_PATH_KEY = "clickhouse.lib.path";
    protected static final String KYLIN_SQL_BASE_DIR = "../kylin-it/src/test/resources/query";

    protected static SparkConf sparkConf;
    protected static SparkSession ss;
    private TestingServer zkTestServer;

    private String[] overlay = new String[0];

    protected static void ensureSparkConf() {
        if (sparkConf == null) {
            sparkConf = new SparkConf().setAppName(RandomUtil.randomUUIDStr()).setMaster("local[4]");
        }
    }

    @BeforeClass
    public static void beforeClass() {

        if (Shell.MAC)
            overwriteSystemPropBeforeClass("org.xerial.snappy.lib.name", "libsnappyjava.jnilib");//for snappy

        ensureSparkConf();
        sparkConf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer");
        sparkConf.set(StaticSQLConf.CATALOG_IMPLEMENTATION().key(), "in-memory");
        sparkConf.set("spark.sql.shuffle.partitions", "1");
        sparkConf.set("spark.memory.fraction", "0.1");
        // opt memory
        sparkConf.set("spark.shuffle.detectCorrupt", "false");
        // For sinai_poc/query03, enable implicit cross join conversion
        sparkConf.set("spark.sql.crossJoin.enabled", "true");
        sparkConf.set(StaticSQLConf.WAREHOUSE_PATH().key(),
                TempMetadataBuilder.TEMP_TEST_METADATA + "/spark-warehouse");

        sparkConf.set("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY");
        sparkConf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED");
        sparkConf.set("spark.sql.legacy.timeParserPolicy", "LEGACY");
        sparkConf.set("spark.sql.parquet.mergeSchema", "true");
        sparkConf.set("spark.sql.legacy.allowNegativeScaleOfDecimal", "true");
        sparkConf.set("spark.sql.broadcastTimeout", "900");
        sparkConf.set("spark.sql.globalTempDatabase", "KYLIN_LOGICAL_VIEW");

        if (!sparkConf.getOption("spark.sql.extensions").isEmpty()) {
            sparkConf.set("spark.sql.extensions",
                    sparkConf.get("spark.sql.extensions") + ", io.delta.sql.DeltaSparkSessionExtension");
        } else {
            sparkConf.set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension");
        }
        sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog");
        sparkConf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false");
        sparkConf.set("spark.databricks.delta.vacuum.parallelDelete.enabled", "true");
        ss = SparkSession.builder().withExtensions(ext -> {
            ext.injectOptimizerRule(ss -> new ConvertInnerJoinToSemiJoin());
            return null;
        }).config(sparkConf).getOrCreate();
        SparderEnv.setSparkSession(ss);
    }

    protected static void configGluten(SparkConf conf) {
        String chLibPath = System.getProperty(GLUTEN_CH_LIB_PATH_KEY);
        if (StringUtils.isEmpty(chLibPath) || !new File(chLibPath).exists()) {
            log.warn("-Dclickhouse.lib.path is not set or path not exists, skip gluten config");
        }
        conf.set("spark.gluten.enabled", "true");
        conf.set("spark.plugins", "io.glutenproject.GlutenPlugin");
        conf.set("spark.gluten.sql.columnar.libpath", chLibPath);
        conf.set("spark.gluten.sql.columnar.extended.columnar.pre.rules", "org.apache.spark.sql.execution.gluten.ConvertKylinFileSourceToGlutenRule");
        conf.set("spark.gluten.sql.columnar.extended.expressions.transformer", "org.apache.spark.sql.catalyst.expressions.gluten.CustomerExpressionTransformer");
        // FIXME, enable this after we fix the issue of AutoSinaiPocTest and AutoTpchTest
        conf.set("spark.sql.adaptive.enabled", "false");
        //        conf.set("spark.eventLog.enabled", "true");
        //        conf.set("spark.eventLog.dir", "file:///tmp/spark-events");
        //        conf.set("spark.eventLog.compress", "true");
        //        conf.set("spark.eventLog.compression.codec", "snappy");

        conf.set("spark.sql.columnVector.offheap.enabled", "true");
        conf.set("spark.memory.offHeap.enabled", "true");
        conf.set("spark.memory.offHeap.size", "1g");
        conf.set("spark.gluten.sql.enable.native.validation", "false");
        conf.set("spark.gluten.sql.columnar.preferColumnar", "false");
        conf.set("spark.shuffle.manager", "org.apache.spark.shuffle.sort.ColumnarShuffleManager");
        conf.set("spark.gluten.sql.columnar.shuffleSplitDefaultSize", "65505");
        conf.set("spark.gluten.sql.columnar.backend.ch.use.v2", "false");
        conf.set("spark.gluten.sql.columnar.iterator", "true");
        conf.set("spark.gluten.sql.columnar.sort", "true");
        conf.set("spark.sql.exchange.reuse", "true");
        conf.set("spark.gluten.sql.columnar.forceshuffledhashjoin", "true");
        conf.set("spark.locality.wait", "0");
        conf.set("spark.locality.wait.node", "0");
        conf.set("spark.locality.wait.process", "0");
        conf.set("spark.sql.autoBroadcastJoinThreshold", "20MB");
        conf.set("spark.gluten.sql.columnar.columnartorow", "true");
        conf.set("spark.gluten.sql.columnar.loadnative", "true");
        conf.set("spark.gluten.sql.columnar.loadarrow", "false");
        conf.set("spark.gluten.sql.columnar.hashagg.enablefinal", "true");
        conf.set("spark.gluten.sql.columnar.separate.scan.rdd.for.ch", "false");
        conf.set("spark.databricks.delta.maxSnapshotLineageLength", "20");
        conf.set("spark.databricks.delta.snapshotPartitions", "1");
        conf.set("spark.databricks.delta.properties.defaults.checkpointInterval", "5");
        conf.set("spark.databricks.delta.stalenessLimit", "3600000");
        conf.set("spark.gluten.sql.columnar.backend.ch.worker.id", "1");
        conf.set("spark.gluten.sql.columnar.coalesce.batches", "false");
        conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.logger.level", "error");
        conf.set("spark.io.compression.codec", "LZ4");
        conf.set("spark.gluten.sql.columnar.shuffle.customizedCompression.codec", "LZ4");
        conf.set("spark.gluten.sql.columnar.backend.ch.customized.shuffle.codec.enable", "true");
        conf.set("spark.gluten.sql.columnar.backend.ch.customized.buffer.size", "4096");
        conf.set("spark.gluten.sql.columnar.backend.ch.files.per.partition.threshold", "5");
        conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.enable_nullable", "true");
        conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.local_engine.settings.metrics_perf_events_enabled", "false");
        conf.set("spark.gluten.sql.columnar.backend.ch.runtime_conf.local_engine.settings.max_bytes_before_external_group_by", "5000000000");
        conf.set("spark.gluten.sql.columnar.maxBatchSize", "65409");
        conf.set("spark.sql.decimalOperations.allowPrecisionLoss", "false");
    }

    @AfterClass
    public static void afterClass() {
        if (ss != null) {
            ss.close();
        }
        FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        // see https://olapio.atlassian.net/browse/KE-42054
        // After the completion of setting the CancelFlag flag in Calcite, certain optimization rules will be canceled,
        // which can affect the execution of subsequent query scenarios in the module. It is necessary to reset the
        // CancelFlag flag after the completion of this particular use case execution.
        CancelFlag.getContextCancelFlag().clearCancel();
    }


    @Before
    public void setUp() throws Exception {
        init();
        overwriteSystemProp("kylin.build.resource.consecutive-idle-state-num", "1");
        overwriteSystemProp("kylin.build.resource.state-check-interval-seconds", "1s");
        overwriteSystemProp("kylin.engine.spark.build-job-progress-reporter", //
                "org.apache.kylin.engine.spark.job.MockJobProgressReport");
        SparkJobFactoryUtils.initJobFactory();
        for (int i = 0; i < 100; i++) {
            try {
                zkTestServer = new TestingServer(RandomUtil.nextInt(7100, 65530), true);
                break;
            } catch (BindException e) {
                log.warn(e.getMessage());
            }
        }
        overwriteSystemProp("kylin.env.zookeeper-connect-string", zkTestServer.getConnectString());
        overwriteSystemProp("kylin.source.provider.9", "org.apache.kylin.engine.spark.mockup.CsvSource");
        JobContextUtil.getJobContext(getTestConfig());
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
        if (zkTestServer != null) {
            zkTestServer.close();
        }
    }

    public String getProject() {
        return "default";
    }

    protected void setOverlay(String... overlay) {
        this.overlay = overlay;
    }

    protected void init() throws Exception {
        overwriteSystemProp("calcite.keep-in-clause", "true");
        this.createTestMetadata(overlay);

        JobContextUtil.getJobContext(getTestConfig());
    }

    public static void populateSSWithCSVData(KylinConfig kylinConfig, String project, SparkSession sparkSession) {

        ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        Preconditions.checkArgument(projectInstance != null);
        for (String table : projectInstance.getTables()) {

            if ("DEFAULT.STREAMING_TABLE".equals(table) || "DEFAULT.TEST_SNAPSHOT_TABLE".equals(table)
                    || table.contains(kylinConfig.getDDLLogicalViewDB())) {
                continue;
            }

            TableDesc tableDesc = NTableMetadataManager.getInstance(kylinConfig, project).getTableDesc(table);
            ColumnDesc[] columns = tableDesc.getColumns();
            StructType schema = new StructType();
            for (ColumnDesc column : columns) {
                schema = schema.add(column.getName(), convertType(column.getType()), false);
            }
            Dataset<Row> ret = sparkSession.read().schema(schema).csv(String.format(Locale.ROOT, CSV_TABLE_DIR, table));
            ret.createOrReplaceTempView(tableDesc.getName());
        }
    }

    private static DataType convertType(org.apache.kylin.metadata.datatype.DataType type) {
        if (type.isTimeFamily())
            return DataTypes.TimestampType;

        if (type.isDateTimeFamily())
            return DataTypes.DateType;

        if (type.isIntegerFamily())
            switch (type.getName()) {
            case "tinyint":
                return DataTypes.ByteType;
            case "smallint":
                return DataTypes.ShortType;
            case "integer":
            case "int4":
                return DataTypes.IntegerType;
            default:
                return DataTypes.LongType;
            }

        if (type.isNumberFamily())
            switch (type.getName()) {
            case "float":
                return DataTypes.FloatType;
            case "double":
                return DataTypes.DoubleType;
            default:
                if (type.getPrecision() == -1 || type.getScale() == -1) {
                    return DataTypes.createDecimalType(19, 4);
                } else {
                    return DataTypes.createDecimalType(type.getPrecision(), type.getScale());
                }
            }

        if (type.isStringFamily())
            return DataTypes.StringType;

        if (type.isBoolean())
            return DataTypes.BooleanType;

        throw new IllegalArgumentException("KAP data type: " + type + " can not be converted to spark's type.");
    }

}
