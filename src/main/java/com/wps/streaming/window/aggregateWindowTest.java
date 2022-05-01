package com.wps.streaming.window;


import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class aggregateWindowTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> numberStream = dataStream.map(line -> Integer.valueOf(line));
        AllWindowedStream<Integer, TimeWindow> windowStream = numberStream.timeWindowAll(Time.seconds(5));

        windowStream.aggregate(new MyAggregate()).print();
        env.execute("aggregateWindowTest");
    }

    private static class MyAggregate implements AggregateFunction<Integer,Tuple2<Integer,Integer>,Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return new Tuple2<>(0,0);
        }

        @Override
        public Tuple2<Integer, Integer> add(Integer element, Tuple2<Integer, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0+1,accumulator.f1+element);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> accumulator) {
            return (double)accumulator.f1/accumulator.f0;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a1, Tuple2<Integer, Integer> b1) {
            return Tuple2.of(a1.f0+b1.f0, a1.f1+b1.f1);
        }
    }
}
