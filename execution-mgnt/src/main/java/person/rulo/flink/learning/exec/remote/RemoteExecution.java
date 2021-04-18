package person.rulo.flink.learning.exec.remote;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcOutputFormat;
import org.apache.flink.types.Row;
import person.rulo.flink.learning.common.jdbc.MySQLTestDBConnConf;

import java.util.ArrayList;
import java.util.List;

/**
 * @author rulo
 * @date 2020/3/20 15:16
 */
public class RemoteExecution {

    public void submitToRemoteEnv() throws Exception {
        String[] jars = {"F:/jars/flink-jdbc_2.12-1.10.0.jar", "F:/jars/mysql-connector-java-5.1.46.jar"};
        ExecutionEnvironment batchEnv = ExecutionEnvironment.createRemoteEnvironment("localhost", 8081, jars);
//        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

        List<Row> arrayList = new ArrayList<>();
        Row row = new Row(3);
        row.setField(0, 1);
        row.setField(1, "alpha");
        row.setField(2, 6);
        arrayList.add(row);

        DataSet<Row> dataSet = batchEnv.fromCollection(arrayList);

        JdbcOutputFormat jdbcOutputFormat = JdbcOutputFormat
                .buildJdbcOutputFormat()
                .setDrivername(MySQLTestDBConnConf.DRIVER)
                .setDBUrl(MySQLTestDBConnConf.URL)
                .setUsername(MySQLTestDBConnConf.USERNAME)
                .setPassword(MySQLTestDBConnConf.PASSWORD)
                .setQuery("insert into student (id, name, age) values (?, ?, ?)")
                .finish();

        dataSet.output(jdbcOutputFormat);

//        dataSet.writeAsText("/home/adam/flink/data.txt");

        batchEnv.execute("submitToRemoteEnv");

    }





}
