package com.wps.streaming.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;


/**
 * 需求：每隔1秒统计最近2秒的单词的次数
 */
public class WordCount1 {
    public static void main(String[] args) throws Exception {
        //步骤一：获取执行环境，获取程序入口
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //步骤二：获取数据源
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        //步骤三：数据的处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
//                    out.collect(new Tuple2<>(word, 1));
                    out.collect(Tuple2.of(word,1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum(1);
        //步骤四：数据输出
        result.print();
        //步骤五：启动应用程序
        env.execute("word count");
    }
}
