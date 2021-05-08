package person.rulo.flink.learning.dstreamapi.example;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import person.rulo.flink.learning.dstreamapi.sink.HiveSink;
import person.rulo.flink.learning.dstreamapi.serialize.ProtobufDeserializationSchema;
import person.rulo.flink.learning.dstreamapi.proto.PushMessageProto;

import java.util.Properties;

/**
 * @Author rulo
 * @Date 2021/5/6 19:09
 */
public class KafkaToHive {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.177.233.91:9092,10.177.233.92:9092,10.177.233.93:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dstream_consumer");
        FlinkKafkaConsumer<PushMessageProto.PushMessageMq> kafkaConsumer = new FlinkKafkaConsumer<>("test_external_user_group", new ProtobufDeserializationSchema(), properties);
        kafkaConsumer.setStartFromLatest();
        DataStream<PushMessageProto.PushMessageMq> kafkaInitStream = env.addSource(kafkaConsumer);
//        kafkaInitStream.print();
        DataStream<Tuple2<String, String>> testStream = kafkaInitStream
//                .filter(pushMessageMq -> pushMessageMq.hasMessageId() && pushMessageMq.hasTargetValue())
                .map(pushMessageMq -> new Tuple2<>(pushMessageMq.getMessageId(), pushMessageMq.getTargetValue()))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));
        testStream.addSink(new HiveSink()).setParallelism(1);
        env.execute();
    }
}
