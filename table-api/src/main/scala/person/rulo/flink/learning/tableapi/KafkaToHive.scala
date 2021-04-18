package person.rulo.flink.learning.tableapi

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.{ExecutionCheckpointingOptions, StreamExecutionEnvironment}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.catalog.hive.HiveCatalog

import java.time.Duration

/**
 * @author rulo
 * @date 2021/4/18 18:49
 */
object KafkaToHive {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val environmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tableEnv = StreamTableEnvironment.create(env, environmentSettings)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    tableEnv.getConfig.getConfiguration.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(10))
    /* Hive Catalog */
    val HIVE_CATALOG = "hive_test"
    val DEFAULT_DATABASE = "test"
    val HIVE_CONF_DIR = "E:\\tmp\\conf"
    //        String HIVE_CONF_DIR = "/mnt/e/tmp/conf";
    val catalog = new HiveCatalog(HIVE_CATALOG, DEFAULT_DATABASE, HIVE_CONF_DIR)
    tableEnv.registerCatalog(HIVE_CATALOG, catalog)
    tableEnv.useCatalog(HIVE_CATALOG)
    /* 构造kafka流表 */
    val tableResult = tableEnv.executeSql("DROP TABLE IF EXISTS kafka_source_table")
    tableResult.print()
    val sqlCreateSourceTable =
      """
        | CREATE TABLE kafka_source_table (
        |   user_id STRING,
        |   order_amount DOUBLE,
        |   log_ts TIMESTAMP(3),
        |   WATERMARK FOR log_ts AS log_ts - INTERVAL '5' SECOND
        | ) WITH (
        |   'connector' = 'kafka',
        |   'topic' = 'topic-object',
        |   'properties.bootstrap.servers' = 'localhost:9092',
        |   'properties.group.id' = 'group_flink_job',
        |   'scan.startup.mode' = 'earliest-offset',
        |   'format' = 'json',
        |   'json.fail-on-missing-field' = 'false',
        |   'json.ignore-parse-errors' = 'true'
        | )
        |""".stripMargin
    val tableResult1 = tableEnv.executeSql(sqlCreateSourceTable)
    tableResult1.print()
    /* 构造Hive目标表 */
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    val tableResult2 = tableEnv.executeSql("DROP TABLE IF EXISTS hive_sink_table")
    tableResult2.print()
    val sqlCreateSinkTable =
      """
        | CREATE TABLE hive_sink_table (
        |   user_id STRING,
        |   order_amount DOUBLE
        | ) PARTITIONED BY (dt STRING, hr STRING) STORED AS parquet TBLPROPERTIES (
        |   'partition.time-extractor.timestamp-pattern'='$dt $hr:00:00',
        |   'sink.partition-commit.trigger' = 'partition-time',
        |   'sink.partition-commit.delay' = '5 s',
        |   'sink.partition-commit.policy.kind' = 'metastore,success-file'
        | )
        |""".stripMargin
    val tableResult3 = tableEnv.executeSql(sqlCreateSinkTable)
    tableResult3.print()
    /* 写入数据 */
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    val sqlInsertIntoSink =
      """
        | INSERT INTO hive_sink_table
        | SELECT user_id, order_amount, DATE_FORMAT(log_ts, 'yyyy-MM-dd'), DATE_FORMAT(log_ts, 'HH')
        | FROM kafka_source_table
        |""".stripMargin
    tableEnv.executeSql(sqlInsertIntoSink)
    val sqlCount = " SELECT user_id, count(user_id) FROM kafka_source_table group by user_id"
    val tableResult4 = tableEnv.executeSql(sqlCount)
    tableResult4.print()
  }
}
