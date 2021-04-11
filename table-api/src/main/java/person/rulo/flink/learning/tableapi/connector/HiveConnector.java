package person.rulo.flink.learning.tableapi.connector;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * @Author rulo
 * @Date 2020/3/20 11:03
 *
 * 使用 java 函数实现的 Hive 连接器
 */
public class HiveConnector {

    EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
    TableEnvironment tableEnv = TableEnvironment.create(settings);

    public void addHiveConnector() throws Exception {
        String name = "hivetest";
        String defaultDatabase = "db_hivetest";
        String hiveConfDir = "F:/tmp/hive-conf";
        String version = "2.3.4";

        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir, version);
        tableEnv.registerCatalog(name, hive);

        tableEnv.useCatalog(name);

    }
}
