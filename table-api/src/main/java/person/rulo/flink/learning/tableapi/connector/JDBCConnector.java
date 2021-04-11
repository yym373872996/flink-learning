package person.rulo.flink.learning.tableapi.connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @Author rulo
 * @Date 2020/3/20 11:03
 *
 * 使用 java 函数实现的 JDBC 连接器
 */
public class JDBCConnector {

    final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

    public void addMySQLConnector() throws Exception {
        // 使用 java 定义 JDBC 连接器的 api 在当前版本还没有发布
//        streamTableEnv.connect(...)
//                .withFormat(...)
//                .withSchema(...)
//                .inAppendMode()
//                .createTemporaryTable("table name");

    }
}
