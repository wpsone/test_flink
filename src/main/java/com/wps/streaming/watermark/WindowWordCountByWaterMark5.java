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

public class WindowWordCountByWaterMark5 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                return "???????????????"+stringLongTuple2.toString();
            }
        }).print();

        env.execute("WindowWordCountByWaterMark5");
    }

    /**
     * IN, OUT, KEY, W
     * IN????????????????????????
     * OUT????????????????????????
     * Key???key?????????????????????Flink?????????String???Tuple?????????
     * W???Window???????????????
     */
    public static class SumProcessWindowFunction extends
            ProcessWindowFunction<Tuple2<String,Long>,String, Tuple, TimeWindow> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        /**
         * ?????????window??????????????????????????????????????????
         * @param tuple key
         * @param context operator????????????
         * @param elements ??????window???????????????
         * @param out ????????????
         */
        @Override
        public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Long>> elements,
                            Collector<String> out) {
            System.out.println("???????????????" + dateFormat.format(context.currentProcessingTime()));
            System.out.println("window start time : " + dateFormat.format(context.window().getStart()));

            List<String> list = new ArrayList<>();
            for (Tuple2<String, Long> ele : elements) {
                list.add(ele.toString() + "|" + dateFormat.format(ele.f1));
            }
            out.collect(list.toString());
            System.out.println("window end time  : " + dateFormat.format(context.window().getEnd()));

        }
    }

    private static class EventTimeExtractor
            implements AssignerWithPeriodicWatermarks<Tuple2<String, Long>> {
        FastDateFormat dateFormat = FastDateFormat.getInstance("HH:mm:ss");
        private long currentMaxEventTime=0L;
        private long maxOufOfOrderness=10000;//??????????????????
        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxEventTime - maxOufOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple2<String, Long> element, long timeStamp) {
            Long currentElementTime = element.f1;
            currentMaxEventTime = Math.max(currentMaxEventTime,currentElementTime);

            long id = Thread.currentThread().getId();

            System.out.println("????????????id="+id+" event = " + element
                    + "|" + dateFormat.format(element.f1) // Event Time
                    + "|" + dateFormat.format(currentMaxEventTime)  // Max Event Time
                    + "|" + dateFormat.format(getCurrentWatermark().getTimestamp())); // Current Watermark

            return currentElementTime;
        }
    }
}
