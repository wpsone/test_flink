package com.wps.streaming.transformation;

import com.wps.batch.basic.WordCount1;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * * 滑动窗口实现单词计数
 *  * 数据源：socket
 *  * 需求：每隔1秒计算最近2秒单词出现的次数
 *  *
 *  * 练习算子：
 *  * flatMap
 *  * keyBy:
 *  *    dataStream.keyBy("someKey") // 指定对象中的 "someKey"字段作为分组key
 *  *    dataStream.keyBy(0) //指定Tuple中的第一个元素作为分组key
 */
public class WindowWordCountJava {
    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("no port set,user default port 9988");
            port=9988;
        }

        //步骤一：获取flink运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "node01";
        String delimiter = "\n";
        //步骤二：获取数据源
        DataStreamSource<String> textStream = env.socketTextStream(hostname, port, delimiter);
        //步骤三：执行transformation操作
        SingleOutputStreamOperator<WordCount> wordCountStream = (SingleOutputStreamOperator<WordCount>) textStream.flatMap(new FlatMapFunction<String, WordCount>() {
            @Override
            public void flatMap(String line, Collector<WordCount> out) throws Exception {
                String[] fields = line.split("\t");
                for (String word : fields) {
                    out.collect(new WordCount(word, 1L));
                }
            }
        }).keyBy("word")
                .timeWindow(Time.seconds(2), Time.seconds(1))
                .sum("count");
        wordCountStream.print().setParallelism(1);
        //步骤四：运行程序
        env.execute("socket word count");
    }

    public static class WordCount {
        public String word;
        public long count;

        public WordCount() {
        }

        public WordCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count='" + count + '\'' +
                    '}';
        }
    }
}
