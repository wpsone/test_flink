package com.wps.streaming.transformation;

import com.wps.streaming.source.MyNoParalleSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class UnionDemo {
    public static void main(String[] args) throws Exception {
        //获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //获取数据源
        DataStreamSource<Long> text1 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        DataStreamSource<Long> text2 = env.addSource(new MyNoParalleSource()).setParallelism(1);
        //把text1和text2组装到一起
        DataStream<Long> text = text1.union(text2);

        SingleOutputStreamOperator<Long> num = text.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("原始接收到数据：" + value);
                return value;
            }
        });
        //每2秒处理一次数据
        SingleOutputStreamOperator<Long> sum = num.timeWindowAll(Time.seconds(2)).sum(0);
        //打印结果
        sum.print().setParallelism(1);
        String jobName = UnionDemo.class.getSimpleName();
        System.out.println(jobName);
        env.execute(jobName);
    }
}
