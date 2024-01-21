package com.pitaya.tutorial.sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description:
 * @Date 2023/09/03 14:55:00
 **/
public class KafkaSinkES {
    public static final String KAFKA_TOPIC = "test";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.56.101:9092")
                .setTopics(KAFKA_TOPIC)
                .setGroupId("consumer_group1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source")
                .name(KAFKA_TOPIC + "-source")
                .uid(KAFKA_TOPIC + "-source")
                .setParallelism(1);

        kafkaStream.sinkTo(new Elasticsearch7SinkBuilder<String>()
                .setBulkFlushMaxActions(1)
                // .setBulkFlushInterval(100)
                // .setBulkFlushMaxSizeMb(1)
                .setHosts(new HttpHost("192.168.56.101", 9200, "http"))
                .setEmitter((element, context, indexer) -> indexer.add(createIndexRequest(element)))
                .build()
        );

        env.execute("kafka sink to ES");
    }

    private static IndexRequest createIndexRequest(String element) {
        return Requests.indexRequest()
                .index("student-test")
                //.id(element)
                .source(element, XContentType.JSON);
    }
}
