package com.pitaya.tutorial.function;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

import org.apache.flink.util.Collector;

/**
 * @Description: 自定义 FlatMapFunction
 * @Date 2023/09/02 09:59:00
 **/
public class WordCountFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] words = s.split(" ");
        // 遍历所有word， 包成二元组输出
        for (String word : words) {
            // collector.collect(new Tuple2<>(str, 1)); // 一样的效果
            collector.collect(Tuple2.of(word, 1));
        }
    }
}
