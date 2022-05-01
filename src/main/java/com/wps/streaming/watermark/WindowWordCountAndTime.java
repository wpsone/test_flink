package com.wps.streaming.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class WindowWordCountAndTime {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word,1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5));

    }

    public static class SumProcessFuntion extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>, Tuple, TimeWindow> {
        FastDateFormat datefomat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple key, Context context, Iterable<Tuple2<String, Integer>> allElements, Collector<Tuple2<String, Integer>> out) throws Exception {
            System.out.println("当前系统时间："+datefomat.format(System.currentTimeMillis()));
            System.out.println("窗口处理时间："+datefomat.format(context.currentProcessingTime()));
            System.out.println("窗口开始时间："+datefomat.format(context.window().getStart()));
            System.out.println("窗口结束时间："+datefomat.format(context.window().getEnd()));
            System.out.println("===============================");
            int count=0;
            for (Tuple2<String, Integer> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(key.getField(0),count));
        }
    }
}
