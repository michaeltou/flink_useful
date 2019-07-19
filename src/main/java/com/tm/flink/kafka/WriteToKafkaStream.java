package com.tm.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

/**
 * created by douming on 2019/7/19.
 * 功能描述：
 */
public class WriteToKafkaStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();



       DataStream< String > messageStream =   env.addSource
                (new SimpleStringGenerator());



        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
                "106.14.95.210:9092",            // broker list
                "flink-write-to-kafka",                  // target topic
                new SimpleStringSchema());   // serialization schema

        messageStream.addSink(myProducer);

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
