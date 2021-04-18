package person.rulo.flink.learning.tableapi.catalogs;


import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;

import java.util.Arrays;

/**
 * @author rulo
 * @date 2020/3/20 10:25
 *
 * 在 Catalogs 里创建 Table
 */
public class TableCreator {

    final ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
    BatchTableEnvironment batchTableEnv = BatchTableEnvironment.create(batchEnv);

    public void createTable() {
        // 定义一条创建 Orders 表的 SQL
        String sqlCreateTableOrders = "CREATE TABLE Orders (`user` BIGINT, product STRING, amount INT)";
        // 通过 sqlUpdate 方法执行表创建语句，创建的表会在 Flink 的 Catalogs 中
        batchTableEnv.sqlUpdate(sqlCreateTableOrders);
        // 输出 Catalogs 中的表
        System.out.println("Tables in catalogs: " + Arrays.toString(batchTableEnv.listTables()));
    }

}
