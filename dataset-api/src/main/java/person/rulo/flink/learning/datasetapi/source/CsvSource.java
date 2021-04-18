package person.rulo.flink.learning.datasetapi.source;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;

/**
 * @author rulo
 * @date 2020/5/6 9:17
 */
public class CsvSource {

    final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    DataSet<Tuple1<String>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
            .types(String.class);

}
