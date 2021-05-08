package person.rulo.flink.learning.dstreamapi.simulator;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import person.rulo.flink.learning.dstreamapi.proto.PushMessageProto;

import java.util.Properties;

/**
 * @Author rulo
 * @Date 2021/5/7 14:14
 */
public class KafkaSender {

    private static Logger logger = LoggerFactory.getLogger(KafkaSender.class);

    public static void main(String[] args) {
        String brokers = "10.177.233.91:9092,10.177.233.92:9092,10.177.233.93:9092";
        String topic = "test_external_user_group";
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
        KafkaProducer<String, byte[]> producer  = new KafkaProducer<>(properties);
        for (int i = 0; i < 100; i++) {
            PushMessageProto.PushMessageMq pushMessageMq = PushMessageProto.PushMessageMq.newBuilder()
                    .setMessageId(String.valueOf(i))
                    .setTaskId(String.valueOf(i))
                    .setTargetType(0)
                    .setTargetValue("a")
                    .setPushType(1)
                    .setTargetCount(i)
                    .build();
            producer.send(new ProducerRecord<>(topic, pushMessageMq.toByteArray()));
            logger.info("sent message: {}", pushMessageMq.toString());
        }
        producer.close();
    }
}
