package person.rulo.flink.learning.exec;

import org.apache.flink.api.common.ExecutionConfig;
import person.rulo.flink.learning.exec.conf.ExecutionConfiguration;
import person.rulo.flink.learning.exec.remote.RemoteExecution;

/**
 * @author rulo
 * @date 2020/3/20 14:38
 */
public class MainRunner {
    public static void main(String[] args) throws Exception {

//        ExecutionConfiguration executionConfiguration = new ExecutionConfiguration();
//        executionConfiguration.printCurrentConfig();

        RemoteExecution remoteExecution = new RemoteExecution();
        remoteExecution.submitToRemoteEnv();
    }

}
