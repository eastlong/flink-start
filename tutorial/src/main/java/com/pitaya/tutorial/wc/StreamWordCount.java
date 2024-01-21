package com.pitaya.tutorial.wc;

import com.pitaya.tutorial.function.WordCountFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Description:
 * @Date 2023/09/02 10:36:00
 **/
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);

        // StreamExecutionEnvironment env = StreamContextEnvironment.createLocalEnvironmentWithWebUI(configuration);
        // StreamExecutionEnvironment env = StreamContextEnvironment.createRemoteEnvironment(configuration);
        StreamExecutionEnvironment env = StreamContextEnvironment.createLocalEnvironmentWithWebUI(configuration);

        // 设置并行度，默认值= 当前计算机的CPU逻辑核数（设置成1即单线程处理）
        // env.setParallelism(32);
        env.setParallelism(1);

        // 从socket文本读取数据
        DataStream<String> inputDataStream = env.socketTextStream("127.0.0.1", 7777);

        String hostname = "192.168.56.101";
        int port = 7777;

        DataStream<String> dataStream = env.socketTextStream(hostname, port);
        // 基于数据流进行转换计算
        DataStream<Tuple2<String, Integer>> resultStream
                = inputDataStream.flatMap(new WordCountFlatMapFunction())
                .keyBy(item -> item.f0)
                // .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .sum(1);
        resultStream.print();
        // 执行任务
        env.execute();
    }
}
