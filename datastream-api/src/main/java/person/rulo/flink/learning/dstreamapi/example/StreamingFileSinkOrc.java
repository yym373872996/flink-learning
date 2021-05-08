package person.rulo.flink.learning.dstreamapi.example;

import org.apache.flink.core.fs.Path;
import org.apache.flink.orc.OrcSplitReaderUtil;
import org.apache.flink.orc.vector.RowDataVectorizer;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.logical.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.orc.TypeDescription;
import person.rulo.flink.learning.dstreamapi.proto.PushMessageProto;
import person.rulo.flink.learning.dstreamapi.serialize.ProtobufDeserializationSchema;

import javax.sql.RowSetMetaData;
import java.util.Properties;
import java.util.regex.Matcher;

/**
 * @Author rulo
 * @Date 2021/5/7 20:54
 */
public class StreamingFileSinkOrc {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setParallelism(1);
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "10.177.233.91:9092,10.177.233.92:9092,10.177.233.93:9092");
        kafkaProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "dstream_consumer");
        FlinkKafkaConsumer<PushMessageProto.PushMessageMq> kafkaConsumer = new FlinkKafkaConsumer<>("test_external_user_group", new ProtobufDeserializationSchema(), kafkaProperties);
        kafkaConsumer.setStartFromLatest();
        DataStream<PushMessageProto.PushMessageMq> kafkaInitStream = env.addSource(kafkaConsumer);
        DataStream<RowData> dataStream = kafkaInitStream
                .map(row -> {
                    GenericRowData rowData = new GenericRowData(3);
                    rowData.setField(0, (int) (Math.random() * 100));
                    rowData.setField(1, Math.random() * 100);
                    rowData.setField(2, StringData.fromString(String.valueOf(Math.random() * 100)));
                    return rowData;
                });
//        DataStream<RowData> dataStream = env.addSource(new MySource());
        final Properties orcProperties = new Properties();
        orcProperties.setProperty("orc.compress", "LZ4");
        LogicalType[] orcTypes = new LogicalType[] {
                new IntType(), new DoubleType(), new VarCharType()
        };
        String[] fields = new String[]{"a1", "b2", "c3"};
        TypeDescription typeDescription = OrcSplitReaderUtil.logicalTypeToOrcType(RowType.of(
                orcTypes, fields
        ));
        final OrcBulkWriterFactory<RowData> factory = new OrcBulkWriterFactory<>(
                new RowDataVectorizer(typeDescription.toString(), orcTypes),
                orcProperties,
                new Configuration()
        );
        StreamingFileSink orcSink = StreamingFileSink
                .forBulkFormat(new Path("file:///E://tmp//orcSink"), factory)
                .build();
        dataStream.addSink(orcSink);
        env.execute();
    }

    public static class MySource implements SourceFunction<RowData> {

        @Override
        public void run(SourceContext<RowData> sourceContext) throws Exception {
            while (true) {
                GenericRowData rowData = new GenericRowData(3);
                rowData.setField(0, (int) (Math.random() * 100));
                rowData.setField(1, Math.random() * 100);
                rowData.setField(2, StringData.fromString(String.valueOf(Math.random() * 100)));
                sourceContext.collect(rowData);
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {

        }
    }
}
