package com.wps.streaming.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

public class WindowWordCountByEventTime {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> dataStream = env.addSource(new TestSource());
        dataStream.map(new MapFunction<String, Tuple2<String,Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0],Long.valueOf(fields[1]));
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(10),Time.seconds(5))
                .process(new SumProcessFunction())
                .print().setParallelism(1);
        env.execute("WindowWordCountAndTime");
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>> {

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(System.currentTimeMillis());
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long l) {
            return element.f1;
        }
    }

    public static class TestSource implements SourceFunction<String> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void run(SourceContext<String> cxt) throws Exception {
            String currTime = String.valueOf(System.currentTimeMillis());
            while (Integer.valueOf(currTime.substring(currTime.length()-4))>400) {
                currTime = String.valueOf(System.currentTimeMillis());
                continue;
            }
            System.out.println("开始发送事件的时间："+dateFormat.format(System.currentTimeMillis()));
            TimeUnit.SECONDS.sleep(3);

            long time = System.currentTimeMillis();
            String event1 = "hadoop,"+ time;
            String event2 = "hadoop,"+ time;
            cxt.collect(event1);
            TimeUnit.SECONDS.sleep(3);
            cxt.collect("hadoop,"+System.currentTimeMillis());
            TimeUnit.SECONDS.sleep(3);
            cxt.collect(event2);
            TimeUnit.SECONDS.sleep(3000000);
        }

        @Override
        public void cancel() {

        }
    }

    public static class SumProcessFunction extends ProcessWindowFunction<Tuple2<String,Long>,Tuple2<String,Integer>, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context,
                            Iterable<Tuple2<String, Long>> allElements,
                            Collector<Tuple2<String, Integer>> out) throws Exception {
            int count = 0;
            for (Tuple2<String, Long> e : allElements) {
                count++;
            }
            out.collect(Tuple2.of(tuple.getField(0),count));
        }
    }
}
