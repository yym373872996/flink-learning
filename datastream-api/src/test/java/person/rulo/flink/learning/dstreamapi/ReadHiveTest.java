package person.rulo.flink.learning.dstreamapi;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import person.rulo.flink.learning.dstreamapi.simulator.KafkaSender;

/**
 * @Author rulo
 * @Date 2021/5/7 15:35
 */
public class ReadHiveTest {

    @Test
    public void readTable() {
        try {
            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            JdbcInputFormat jdbcInputFormat = JdbcInputFormat.buildJdbcInputFormat()
                    .setDrivername("org.apache.hive.jdbc.HiveDriver")
                    .setDBUrl("jdbc:hive2://10.177.168.176:10000")
                    .setUsername("")
                    .setPassword("")
                    .setQuery("select statis_id, statis_type from test.hive_sink_table")
                    .setRowTypeInfo(new RowTypeInfo(TypeInformation.of(String.class), TypeInformation.of(String.class)))
                    .finish();
            DataSource dataSource = env.createInput(jdbcInputFormat);
            dataSource.print();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
