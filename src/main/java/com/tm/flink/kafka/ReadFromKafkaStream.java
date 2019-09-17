package com.tm.flink.kafka;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * created by douming on 2019/7/19.
 * 功能描述：从kafka读取数据
 */
public class ReadFromKafkaStream {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "106.14.95.210:9092");
       // properties.setProperty("zookeeper.connect", "106.14.95.210:2181");
        //指定消费者的groupId
        properties.setProperty("group.id", "testgroup");

        //会自动创建topic
        DataStream< String > messageStream = env.addSource
                (new FlinkKafkaConsumer<String>("flink-test",
                        new SimpleStringSchema(), properties));
        messageStream.rebalance().map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return "Flink read from kafka,says:" + value;
            }
        }).print();


        env.execute();
    }
}
