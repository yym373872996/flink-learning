package person.rulo.flink.learning.tableapi.example;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.catalog.hive.HiveCatalog;

import java.time.Duration;

/**
 * @Author rulo
 * @Date 2021/4/10 13:19
 */
public class KafkaToHive {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
        tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10));
        /* Hive Catalog */
        String HIVE_CATALOG = "hive_test";
        String DEFAULT_DATABASE = "test";
        String HIVE_CONF_DIR = "E:\\tmp\\conf";
//        String HIVE_CONF_DIR = "/mnt/e/tmp/conf";
        Catalog catalog = new HiveCatalog(HIVE_CATALOG, DEFAULT_DATABASE, HIVE_CONF_DIR);
        tableEnv.registerCatalog(HIVE_CATALOG, catalog);
        tableEnv.useCatalog(HIVE_CATALOG);
        /* 构造kafka流表 */
        TableResult tableResult = tableEnv.executeSql("DROP TABLE IF EXISTS kafka_source_table");
        tableResult.print();
        TableResult tableResult1 = tableEnv.executeSql(
                "CREATE TABLE kafka_source_table (\n" +
                        "  user_id STRING,\n" +
                        "  order_amount DOUBLE,\n" +
                        "  log_ts TIMESTAMP(3),\n" +
                        "  WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND\n" +
                        " ) WITH (\n" +
                        "     'connector' = 'kafka',\n" +
                        "     'topic' = 'topic-object',\n" +
                        "     'properties.bootstrap.servers' = 'localhost:9092',\n" +
                        "     'properties.group.id' = 'group_flink_job',\n" +
                        "     'scan.startup.mode' = 'earliest-offset',\n" +
                        "     'format' = 'json',\n" +
                        "     'json.fail-on-missing-field' = 'false',\n" +
                        "     'json.ignore-parse-errors' = 'true'\n" +
                        "   )");
        tableResult1.print();
        /* 构造Hive目标表 */
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
        TableResult tableResult2 = tableEnv.executeSql("DROP TABLE IF EXISTS hive_sink_table");
        tableResult2.print();
        TableResult tableResult3 = tableEnv.executeSql(
                "CREATE TABLE hive_sink_table (\n" +
                        " user_id STRING,\n" +
                        " order_amount DOUBLE\n" +
                        ") PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (\n" +
                        "  'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',\n" +
                        "  'sink.partition-commit.trigger' = 'partition-time',\n" +
                        "  'sink.partition-commit.delay' = '5 s',\n" +
                        "  'sink.partition-commit.policy.kind' = 'metastore,success-file'\n" +
                        ")");
        tableResult3.print();

        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
        tableEnv.executeSql(
                "INSERT INTO hive_sink_table \n" +
                        " SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH') \n" +
                        " FROM kafka_source_table"
        );
        TableResult tableResult4 = tableEnv.executeSql(
                        " SELECT user_id, count(user_id) \n" +
                        " FROM kafka_source_table group by user_id"
        );
        tableResult4.print();

    }
}
