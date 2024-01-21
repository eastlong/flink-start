package com.pitaya.tutorial.sink;

import com.google.gson.Gson;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Description: flink 消费 kafka 数据并写入 redis
 * @Date 2023/09/02 21:54:00
 **/
public class KafkaSinkRedis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("192.168.56.101:9092")
                .setTopics("test")
                .setGroupId("consumer_group1")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> kafkaStream = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");

        // 将消息写入Redis
        FlinkJedisPoolConfig jedisPoolConfig = new FlinkJedisPoolConfig.Builder()
                .setHost("192.168.56.101")
                .setPort(6379)
                .build();

        kafkaStream.addSink(new RedisSink<>(jedisPoolConfig, new RedisExampleMapper()));

        env.execute("Kafka to Redis Example");
    }

    public static class RedisExampleMapper implements RedisMapper<String> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            // 使用RPUSH命令将消息写入Redis列表
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(String data) {
            Gson gson = new Gson();
            Student student = gson.fromJson(data, Student.class);
            // 设置Redis键
            return student.getName();
        }

        @Override
        public String getValueFromData(String data) {
            Gson gson = new Gson();
            Student student = gson.fromJson(data, Student.class);
            // 设置Redis值
            return student.getAge().toString();
        }
    }

    class Student {
        private String name;
        private Integer age;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }
}
