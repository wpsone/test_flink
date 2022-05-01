package com.wps.streaming.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class SocketDemoFullAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));
        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(5));

        windowResult.process(new ProcessAllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void process(Context context, Iterable<Integer> iterable, Collector<Integer> collector) throws Exception {
                System.out.println("执行计算逻辑");
                int count = 0;
                Iterator<Integer> numberIter = iterable.iterator();
                while (numberIter.hasNext()) {
                    Integer number = numberIter.next();
                    count += number;
                }
                collector.collect(count);
            }
        }).print();

        env.execute("SocketDemoFullAgg");
    }
}
