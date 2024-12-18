package person.rulo.flink.learning.dstreamapi.metric;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class MetricsExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        DataStreamSource<String> source = env.socketTextStream("localhost", 9999);
        source.process(new ProcessFunction<String, Object>() {

            LongCounter longCounter;
            MyLongGauge myLongGauge;

            @Override
            public void open(Configuration parameters) throws Exception {
                longCounter = getRuntimeContext().getLongCounter("my-long-counter");
                myLongGauge = getRuntimeContext().getMetricGroup().gauge("my-long-gauge", new MyLongGauge());
            }

            @Override
            public void processElement(String s, ProcessFunction<String, Object>.Context context, Collector<Object> collector) throws Exception {
                longCounter.add(1);
                myLongGauge.add(2L);
                collector.collect(s.toUpperCase());
            }
        });
        env.execute();
    }

    static class MyLongGauge implements Gauge<Long> {

        Long counter = 0L;

        public void add(Long x) {
            counter += x;
        }

        @Override
        public Long getValue() {
            return counter;
        }
    }


}
