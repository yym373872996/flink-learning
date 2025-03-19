package person.rulo.flink.learning.tableapi.example;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class KafkaUpsert {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> source1 = env.socketTextStream("localhost", 9999);
        SingleOutputStreamOperator<User> userStream = source1.map(s -> {
            String[] split = s.split(",");
            User user = new User();
            user.setUserId(Long.parseLong(split[0]));
            user.setUserName(split[1]);
            return user;
        });
        DataStreamSource<String> source2 = env.socketTextStream("localhost", 9998);
        SingleOutputStreamOperator<Behavior> behaviorStream = source2.map(s -> {
            String[] split = s.split(",");
            Behavior behavior = new Behavior();
            behavior.setUserId(Long.parseLong(split[0]));
            behavior.setItemId(Long.parseLong(split[1]));
            behavior.setBehavior(split[2]);
            return behavior;
        });
        tableEnv.createTemporaryView("user_table", userStream);
        tableEnv.createTemporaryView("behavior_table", behaviorStream);
        tableEnv.executeSql(
                "CREATE TABLE kafka_table (\n" +
                        "  `user_id` BIGINT,\n" +
                        "  `user_name` STRING,\n" +
                        "  `item_id` BIGINT,\n" +
                        "  `behavior` STRING,\n" +
                        "  PRIMARY KEY (user_id) NOT ENFORCED\n" +
                        ") WITH (\n" +
                        "  'connector' = 'upsert-kafka',\n" +
                        "  'topic' = 'user-behavior',\n" +
                        "  'properties.bootstrap.servers' = 'localhost:9092',\n" +
                        "  'key.format' = 'json',\n" +
                        "  'value.format' = 'json'\n" +
                        ")"
        );
//        tableEnv.executeSql("select t1.*, t2.* from user_table t1 left join behavior_table t2 on t1.userId = t2.userId")/*.print()*/;
        tableEnv.executeSql(
                "insert into kafka_table\n" +
                        "select \n" +
                        "t1.userId as user_id,\n" +
                        "t1.userName as user_name,\n" +
                        "t2.itemId as item_id,\n" +
                        "t2.behavior as behavior\n" +
                        "from user_table t1\n" +
                        "left join behavior_table t2\n" +
                        "on t1.userId = t2.userId"
        );
        tableEnv.executeSql("select * from kafka_table").print();
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class User {
        private Long userId;
        private String userName;
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Behavior {
        private Long userId;
        private Long itemId;
        private String behavior;
    }

}
