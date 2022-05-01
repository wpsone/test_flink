package com.wps.streaming.window;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.runtime.operators.windowing.TimestampedValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class CountWindowWordCountByEvictor {
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

        WindowedStream<Tuple2<String, Integer>, Tuple, GlobalWindow> keyedWindow = stream.keyBy(0)
                .window(GlobalWindows.create())
                .trigger(new MyCountTrigger(2))
                .evictor(new MyCountEvictor(3));

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordCounts = keyedWindow.sum(1);
        wordCounts.print().setParallelism(1);
        env.execute("CountWindowWordCountByEvictor");
    }

    public static class MyCountTrigger extends Trigger<Tuple2<String,Integer>, GlobalWindow> {
        private long maxCount;

        private ReducingStateDescriptor<Long> stateDescriptor =
                new ReducingStateDescriptor<Long>("count",new ReduceFunction<Long>() {

                    @Override
                    public Long reduce(Long aLong, Long t1) throws Exception {
                        return aLong + t1;
                    }
                },Long.class);

        public MyCountTrigger(long maxCount) {
            this.maxCount = maxCount;
        }

        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
            ReducingState<Long> count = ctx.getPartitionedState(stateDescriptor);
            count.add(1L);
            if (count.get() == maxCount) {
                count.clear();
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long l, GlobalWindow globalWindow, TriggerContext triggerContext) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(GlobalWindow globalWindow, TriggerContext tc) throws Exception {
            tc.getPartitionedState(stateDescriptor).clear();
        }
    }

    private static class MyCountEvictor implements Evictor<Tuple2<String,Integer>,GlobalWindow> {
        private long windowCount;

        public MyCountEvictor(long windowCount) {
            this.windowCount = windowCount;
        }


        @Override
        public void evictBefore(Iterable<TimestampedValue<Tuple2<String, Integer>>> elements, int size, GlobalWindow window, EvictorContext evictorContext) {
            if (size <= windowCount) {
                return;
            } else {
                int evictorCount = 0;
                Iterator<TimestampedValue<Tuple2<String, Integer>>> iterator = elements.iterator();
                while (iterator.hasNext()) {
                    iterator.next();
                    evictorCount++;
                    if (size - evictorCount < windowCount) {
                        break;
                    } else {
                        iterator.remove();
                    }
                }
            }
        }

        @Override
        public void evictAfter(Iterable<TimestampedValue<Tuple2<String, Integer>>> iterable, int i, GlobalWindow globalWindow, EvictorContext evictorContext) {

        }
    }
}
