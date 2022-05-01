package com.wps.streaming.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

public class WindowWordCountBySource2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.addSource(new TestSource());
        dataStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word,1));
                }
            }
        }).keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .process(new SumProcessFunction())
                .print().setParallelism(1);

        env.execute("WindowWordCountBySource2");
    }

    public static class TestSource implements SourceFunction<String> {
        FastDateFormat dateFormat = FastDateFormat.getInstance();

        @Override
        public void run(SourceContext<String> cxt) throws Exception {
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length()-4))>100) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }
            System.out.println("开始发送事件的时间："+dateFormat.format(System.currentTimeMillis()));

            TimeUnit.SECONDS.sleep(3);
            String event1 = "hadoop";
            String event2 = "hadoop";

            cxt.collect(event1);

            TimeUnit.SECONDS.sleep(3);
            cxt.collect("hadoop");
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event2);

            TimeUnit.SECONDS.sleep(3000000);
        }

        @Override
        public void cancel() {

        }


    }

    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String,Integer>,Tuple2<String,Integer>, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Integer>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int count =0;
            for (Tuple2<String, Integer> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(tuple.getField(0),count));
        }
    }
}
