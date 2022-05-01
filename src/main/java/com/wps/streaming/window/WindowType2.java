package com.wps.streaming.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WindowType2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> stream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    collector.collect(Tuple2.of(word, 1));
                }
            }
        });
//        System.out.println("滑动窗口");
        stream.keyBy(0)
                .timeWindow(Time.seconds(6),Time.seconds(4))
                .sum(1)
                .print();

//        System.out.println("滚动窗口");
        stream.keyBy(0)
                .timeWindow(Time.seconds(6))
                .sum(1)
                .print();

//        System.out.println("会话滚动窗口");
        stream.keyBy(0)
                .countWindow(6,4).sum(1).print();

//        System.out.println("会话滑动窗口");
        stream.keyBy(0)
                .countWindow(6)
                .sum(1)
                .print();

//        System.out.println("滚动窗口2");
        stream.keyBy(0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .sum(1)
                .print();

//        System.out.println("滑动窗口2");
        stream.keyBy(0)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(6),Time.seconds(4)))
                .sum(1)
                .print();

        env.execute("word count");
    }
}
