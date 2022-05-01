package com.wps.streaming.source;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class StreamingSourceFromCollection {
    public static void main(String[] args) throws Exception {
        //步骤一：获取环境变量
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //步骤二：模拟数据
        ArrayList<String> data = new ArrayList<>();
        data.add("hadoop");
        data.add("spark");
        data.add("flink");

        //步骤三：获取数据源
        DataStreamSource<String> dataStream = env.fromCollection(data);

        //步骤四：transformation操作
        SingleOutputStreamOperator<String> addPreStream = dataStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String word) throws Exception {
                return "kaikeba_" + word;
            }
        });

        //步骤五：对结果进行处理
        addPreStream.print().setParallelism(1);

        //步骤六：启动程序
        env.execute("StreamingSourceFromCollection");
    }
}
