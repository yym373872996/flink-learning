package person.rulo.flink.learning.exec.conf;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author rulo
 * @Date 2020/3/20 14:32
 */
public class ExecutionConfiguration {

    final StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

    public void printCurrentConfig() {
        ExecutionConfig executionConfig = streamEnv.getConfig();
        System.out.println(executionConfig.toString());
    }
}
