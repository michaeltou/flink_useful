package com.tm.flink.kafka;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import java.util.*;

/**
 * created by douming on 2019/8/06.
 * 功能描述：flink写入elasticsearch
 */
@SuppressWarnings("all")
public class WriteToElasticsearchWithTimestamp {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000); // checkpoint every 5000 msecs


       DataStream< String > input =   env.addSource
                (new SimpleStringGenerator());



        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("127.0.0.1", 9200, "http"));
        httpHosts.add(new HttpHost("106.14.98.121", 9200, "http"));

// use a ElasticsearchSink.Builder to create an ElasticsearchSink
        ElasticsearchSink.Builder<String> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<String>() {
                    public IndexRequest createIndexRequest(String element) {

                        Map<String, Object> son = new HashMap<>();
                        son.put("name","this is son name");
                        son.put("uuid",UUID.randomUUID().toString());

                        Map<String, Object> json = new HashMap<>();
                        json.put("data", element);
                        Date currentTime = new Date();
                    /*   SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                         String dateString = formatter.format(currentTime);*/
                        json.put("timestamp", System.currentTimeMillis());
                        json.put("son",son);

                        Random r = new Random();
                        int target1 = r.nextInt(200000);
                        int target2 = r.nextInt(1000000);
                        json.put("target1",target1);
                        json.put("target2",target2);


                        return Requests.indexRequest()
                                .index("my-flink-index")
                                .type("my-type")
                                .source(json);
                    }

                    @Override
                    public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createIndexRequest(element));
                    }
                }
        );

// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

// provide a RestClientFactory for custom configuration on the internally created REST client
/*        esSinkBuilder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setDefaultHeaders(...)
                    restClientBuilder.setMaxRetryTimeoutMillis(...)
                    restClientBuilder.setPathPrefix(...)
                    restClientBuilder.setHttpClientConfigCallback(...)
                }
        );*/

// finally, build and add the sink to the job's pipeline
        input.addSink(esSinkBuilder.build());

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
                Random r = new Random();
                int interval = r.nextInt(2000);
                Thread.sleep(interval);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}
