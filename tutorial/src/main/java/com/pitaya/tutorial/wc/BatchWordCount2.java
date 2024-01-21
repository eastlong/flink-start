package com.pitaya.tutorial.wc;

import com.pitaya.tutorial.function.WordCountFlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Description: WordCount简写
 * @Date 2023/09/02 10:10:00
 **/
public class BatchWordCount2 {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据  按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("input/words.txt");
        // 对数据集进行处理，按空格分词展开，转换成(word, 1)二元组进行统计
        // 按照第一个位置的word分组
        // 按照第二个位置上的数据求和
        lineDS.flatMap(new WordCountFlatMapFunction())
                .groupBy(0)
                .sum(1)
                .print();
    }
}
