package com.wps.streaming.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class SocketDemoIncrAgg {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStream = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Integer> intDStream = dataStream.map(number -> Integer.valueOf(number));

        AllWindowedStream<Integer, TimeWindow> windowResult = intDStream.timeWindowAll(Time.seconds(5));
        windowResult.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer last, Integer current) throws Exception {
                System.out.println("执行逻辑"+last+" "+current);
                return last+current;
            }
        }).print();

        env.execute(SocketDemoIncrAgg.class.getSimpleName());
    }
}
