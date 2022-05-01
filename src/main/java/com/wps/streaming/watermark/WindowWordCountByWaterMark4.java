package com.wps.streaming.watermark;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class WindowWordCountByWaterMark4 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(1000);

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<Tuple2<String, Long>>("late-date") {
        };

        SingleOutputStreamOperator<String> result = dataStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                String[] fields = line.split(",");
                return new Tuple2<>(fields[0], Long.valueOf(fields[1]));
            }
        }).assignTimestampsAndWatermarks(new EventTimeExtractor())
                .keyBy(0)
                .timeWindow(Time.seconds(3))
                .sideOutputLateData(outputTag)
                .process(new SumProcessWindowFunction());
        result.print();

        result.getSideOutput(outputTag).map(new MapFunction<Tuple2<String, Long>, String>() {
            @Override
            public String map(Tuple2<String, Long> stringLongTuple2) throws Exception {
                return "迟到数据："+stringLongTuple2.toString();
            }
        }).print();

        env.execute("WindowWordCountByWaterMark4");
    }

    private static class EventTimeExtractor implements AssignerWithPeriodicWatermarks<Tuple2<String,Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime=0L;
        private long maxOutOfOrderness=10000;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long l) {
            Long currentElementTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime,currentElementTime);
            System.out.println("event = "+element
            +"|" + dateFormat.format(element.f1)
            +"|" + dateFormat.format(currentMaxEventTime)
            +"|" + dateFormat.format(getCurrentWatermark().getTimestamp()));
            return currentElementTime;
        }
    }

    public static class SumProcessWindowFunction extends ProcessWindowFunction<Tuple2<String,Long>,String, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");

        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements, Collector<String> out) throws Exception {
            System.out.println("处理时间："+dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time : "+dateFormat.format(context.window().getStart()));

            List<Object> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString()+"|"+dateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("window end time  : "+dateFormat.format(context.window().getEnd()));
        }
    }
}
