package person.rulo.flink.learning.cdc;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ReadMySQLExample {

    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.enableCheckpointing(5000);
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        tableEnv.executeSql("CREATE TABLE flink_score (\n" +
                "     id INT,\n" +
                "     name STRING,\n" +
                "     gender STRING,\n" +
                "     score DOUBLE,\n" +
                "     PRIMARY KEY(id) NOT ENFORCED\n" +
                "     ) WITH (\n" +
                "     'connector' = 'mysql-cdc',\n" +
                "     'hostname' = 'localhost',\n" +
                "     'port' = '3306',\n" +
                "     'username' = 'root',\n" +
                "     'password' = '1qazXSW@',\n" +
                "     'database-name' = 'dev',\n" +
                "     'table-name' = 't_score')");
        // 直接查询源表
//        tableEnv.executeSql("select * from flink_score").print();’
        // 分组聚合
//        tableEnv.executeSql("select gender, avg(score) as avg_score from flink_score group by gender").print();
        // 开窗topN
        tableEnv.executeSql("select gender, name, sum_score, rn\n" +
                "from \n" +
                "(\n" +
                "select gender, name, sum_score,\n" +
                "row_number() over(partition by gender order by sum_score desc) as rn\n" +
                "from\n" +
                "(\n" +
                "     select gender, name, sum(score) as sum_score\n" +
                "     from flink_score\n" +
                "     group by gender, name\n" +
                ")\n" +
                ")\n" +
                "where rn <=2").print();
    }

}
