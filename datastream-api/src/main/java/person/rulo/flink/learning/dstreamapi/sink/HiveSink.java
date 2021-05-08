package person.rulo.flink.learning.dstreamapi.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @Author rulo
 * @Date 2021/5/6 18:47
 */
public class HiveSink extends RichSinkFunction<Tuple2<String, String>> {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String driverName = "org.apache.hive.jdbc.HiveDriver";
        String dbUrl = "jdbc:hive2://10.177.168.176:10000";
        String userName = "hadoop";
        String password = "";
        Class.forName(driverName);
        connection = DriverManager.getConnection(dbUrl, userName, password);
        logger.info("Connection initialized: {}", connection.getSchema());
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        logger.info("Execute statement");
        String messageId = value.f0;
        String content = value.f1;
        String sql = "insert into test.push_sink(message_id, content) values (?, ?)";
        preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, messageId);
        preparedStatement.setString(2, content);
        preparedStatement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }
}
