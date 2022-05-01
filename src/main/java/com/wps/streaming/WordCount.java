package com.wps.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

//        env.setStateBackend(new MemoryStateBackend());
//        env.setStateBackend(new FsStateBackend("hdfs://xxx"));
//        env.setStateBackend(new RocksDBStateBackend(new FsStateBackend("")));

        DataStreamSource<String> dataStream = env.socketTextStream("192.168.52.100", 8888);
        SingleOutputStreamOperator<Tuple2<String, Long>> wordOneStream = dataStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                String[] fields = line.split(",");
                for (String word : fields) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Long>> result = wordOneStream.keyBy(0)
                .sum(1);

        result.print();
        env.execute(WordCount.class.getSimpleName());
    }
}
