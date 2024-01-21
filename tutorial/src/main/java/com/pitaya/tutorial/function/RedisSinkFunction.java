package com.pitaya.tutorial.function;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;


/**
 * @Description:
 * @Date 2023/09/02 22:43:00
 **/
public class RedisSinkFunction extends RichSinkFunction<Tuple2<String, String>> {
    private transient Jedis jedis;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 连接redis
        jedis = new Jedis("192.168.56.101", 6379, 5000);
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }

    @Override
    public void invoke(Tuple2<String, String> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }

        jedis.set(value.f0, value.f1);
    }
}
