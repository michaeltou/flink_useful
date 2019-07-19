package com.tm.flink.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * created by douming on 2019/7/19.
 * 功能描述：
 */
public class ReadThenWriteToKafkaStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "106.14.95.210:9092");
        // properties.setProperty("zookeeper.connect", "106.14.95.210:2181");
        properties.setProperty("group.id", "mygroup");

        DataStream< String > messageStream = env.addSource
                (new FlinkKafkaConsumer<String>("test",
                        new SimpleStringSchema(), properties));
        DataStream< String > resultStream =     messageStream.rebalance().map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Kafka and Flink says: " + value;
            }
        }) ;

        resultStream.print();

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "106.14.95.210:9092",            // broker list
                "flink-write-to-kafka",                  // target topic
                new SimpleStringSchema());   // serialization schema

        resultStream.addSink(myProducer);

        env.execute();
    }


    public static class SimpleStringGenerator implements SourceFunction<String> {
        private static final long serialVersionUID = 2174904787118597072L;
        boolean running = true;
        long i = 0;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            while(running) {
                ctx.collect("element-"+ (i++));
                Thread.sleep(10);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
