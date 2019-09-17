package com.tm.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * created by douming on 2019/7/17.
 * 功能描述：从collection 数据源，进行流式计算
 */
public class CollectionWindowWordCount {

    /**

     * 1: start this main class, then get output.
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<Tuple1<String>> aList = new ArrayList<Tuple1<String>>();
        for(int i=0;i<1000;i++){
            aList.add(new Tuple1<>("gao dan dan"));
            aList.add(new Tuple1<>("insert into that place"));
        }

        DataStream<Tuple1<String>> textStream = env.fromCollection(aList);
        DataStream<Tuple2<String, Integer>> wordCounts = textStream
                .flatMap(new FlatMapFunction<Tuple1<String>, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(Tuple1<String> text, Collector<Tuple2<String, Integer>> out) {
                        String value = text.getField(0);
                        //System.out.println("xxx the value is "+ value);
                        for (String word : value.split("\\s")) {
                            out.collect(Tuple2.of(word, 1));
                        }
                    }
                });

      //  wordCounts.print().setParallelism(1);

       DataStream<Tuple2<String, Integer>> result = wordCounts
                .keyBy(0)
                .timeWindow(Time.milliseconds(3))
                .sum(1);

        result.print().setParallelism(1);


        env.execute("CollectionWindowWordCount");
        Thread.sleep(30000);
    }

}
