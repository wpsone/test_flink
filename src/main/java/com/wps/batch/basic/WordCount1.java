package com.wps.batch.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.concurrent.ExecutionException;

public class WordCount1 {
    public static void main(String[] args) throws Exception {
        //步骤一：获取离线程序入口
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String inputPath = "E:\\DataFiles\\kkb_data\\hello.txt";
        //步骤二：数据输入
        DataSet<String> dataSet = env.readTextFile(inputPath);
        //步骤三：数据处理
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOneDataSet = dataSet.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(new Tuple2<String, Integer>(word, 1));
                }
            }
        });

        AggregateOperator<Tuple2<String, Integer>> result = wordAndOneDataSet.groupBy(0)
                .sum(1);
        //步骤四：数据结果处理
        result.writeAsText("E:\\DataFiles\\kkb_data\\result.txt").setParallelism(1);
        //步骤五：启动程序
        env.execute("word count");
    }
}
