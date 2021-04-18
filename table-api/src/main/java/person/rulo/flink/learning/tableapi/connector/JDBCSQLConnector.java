package person.rulo.flink.learning.tableapi.connector;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import person.rulo.flink.learning.common.jdbc.MySQLTestDBConnConf;
import person.rulo.flink.learning.common.jdbc.PostgreSQLTestDBConnConf;

import java.util.Arrays;

/**
 * @author rulo
 * @date 2020/3/20 10:18
 *
 * 使用 SQL 方式实现的 JDBC 连接器
 */
public class JDBCSQLConnector {

    // SQL 方式创建的 Table 是流式的，所以要用 StreamTableEnvironment 去操作 Table 对象
    final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
    StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv);

    /**
     * 用连接器创建一个 MySQL 表的连接，并实现 MySQL 表数据的查询
     * @throws Exception
     */
    public void createMySQLTable() throws Exception {
        // 定义一条 Flink SQL 实现的建表语句，其中包含了 JDBC 的连接配置信息
        String sqlCreateTableStudent1 = "" +
                "CREATE TABLE student1 (" +
                "`id` INT, name STRING, age INT" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = '" + MySQLTestDBConnConf.URL + "'," +
                "'connector.table' = 'student1'," +
                "'connector.driver' = '" + MySQLTestDBConnConf.DRIVER + "'," +
                "'connector.username' = '" + MySQLTestDBConnConf.USERNAME + "'," +
                "'connector.password' = '" + MySQLTestDBConnConf.PASSWORD + "'" +
                ")";
        streamTableEnv.sqlUpdate(sqlCreateTableStudent1);
        // 输出 Catalogs 中的表
        System.out.println("Tables in catalogs: " + Arrays.toString(streamTableEnv.listTables()));
        // 定义表数据查询语句
        String sqlSelectTableStudent1 = "" +
                "SELECT * FROM student1";
        // 执行 SQL 查询并将结果存到 Table 对象中
        Table tableStudent1 = streamTableEnv.sqlQuery(sqlSelectTableStudent1);
        // 打印 Table 的 schema
        tableStudent1.printSchema();
        // 将 Table 中的数据追加到一个新的 DataStream 中
        DataStream dataStreamStudent1 = streamTableEnv.toAppendStream(tableStudent1, Row.class);
        // 打印 DataStream 中的数据
        dataStreamStudent1.print();
        // 执行任务
        streamEnv.execute("createMySQLTable");
    }

    public void mySQL2MySQL() throws Exception {

        String sqlCreateTableStudent1 = "" +
                "CREATE TABLE student1 (" +
                "`id` INT, name STRING, age INT" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = '" + MySQLTestDBConnConf.URL + "'," +
                "'connector.table' = 'student1'," +
                "'connector.driver' = '" + MySQLTestDBConnConf.DRIVER + "'," +
                "'connector.username' = '" + MySQLTestDBConnConf.USERNAME + "'," +
                "'connector.password' = '" + MySQLTestDBConnConf.PASSWORD + "'" +
                ")";
        streamTableEnv.sqlUpdate(sqlCreateTableStudent1);

        String sqlCreateTableStudent2 = "" +
                "CREATE TABLE student2 (" +
                "`id` INT, name STRING, age INT" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = '" + MySQLTestDBConnConf.URL + "'," +
                "'connector.table' = 'student2'," +
                "'connector.driver' = '" + MySQLTestDBConnConf.DRIVER + "'," +
                "'connector.username' = '" + MySQLTestDBConnConf.USERNAME + "'," +
                "'connector.password' = '" + MySQLTestDBConnConf.PASSWORD + "'" +
                ")";
        streamTableEnv.sqlUpdate(sqlCreateTableStudent2);

        // 输出 Catalogs 中的表
        System.out.println("Tables in catalogs: " + Arrays.toString(streamTableEnv.listTables()));
        // 定义一条 INSERT 语句将表 student1 的数据查询出并插入到表 student2 中
        String insertTo = "" +
                "INSERT INTO student2 SELECT id, name, age FROM student1";
        streamTableEnv.sqlUpdate(insertTo);
        // 执行任务
        streamEnv.execute("mySQL2MySQL");

    }

    /**
     * 用连接器创建一个 PostgreSQL 表的连接，并实现 PostgreSQL 表数据的查询
     * @throws Exception
     */
    public void createPostgreSQLTable() throws Exception {
        // 定义一条 Flink SQL 实现的建表语句，其中包含了 JDBC 的连接配置信息
        String sqlCreateTableStudent = "" +
                "CREATE TABLE student (" +
                "`id` INT, name STRING, age INT" +
                ") WITH (" +
                "'connector.type' = 'jdbc'," +
                "'connector.url' = '" + PostgreSQLTestDBConnConf.URL + "'," +
                "'connector.table' = 'student'," +
                "'connector.driver' = '" + PostgreSQLTestDBConnConf.DRIVER + "'," +
                "'connector.username' = '" + PostgreSQLTestDBConnConf.USERNAME + "'," +
                "'connector.password' = '" + PostgreSQLTestDBConnConf.PASSWORD + "'" +
                ")";
        streamTableEnv.sqlUpdate(sqlCreateTableStudent);
        // 输出 Catalogs 中的表
        System.out.println("Tables in catalogs: " + Arrays.toString(streamTableEnv.listTables()));
        // 定义表数据查询语句
        String sqlSelectTableStudent = "" +
                "SELECT * FROM student";
        // 执行 SQL 查询并将结果存到 Table 对象中
        Table tableStudent = streamTableEnv.sqlQuery(sqlSelectTableStudent);
        // 打印 Table 的 schema
        tableStudent.printSchema();
        // 将 Table 中的数据追加到一个新的 DataStream 中
        DataStream dataStreamStudent = streamTableEnv.toAppendStream(tableStudent, Row.class);
        // 打印 DataStream 中的数据
        dataStreamStudent.print();
        // 执行任务
        streamEnv.execute("createPostgreSQLTable");
    }

}
