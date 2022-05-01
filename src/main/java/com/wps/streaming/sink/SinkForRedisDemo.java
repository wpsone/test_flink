package com.wps.streaming.sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import scala.Tuple2;

/**
 * 把数据写入redis
 */
public class SinkForRedisDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> text = env.socketTextStream("localhost", 8888, "\n");

        //lpush l_words word
        //对数据进行组装，把string转化为tuple2<String,String>
        DataStream<Tuple2<String, String>> l_words = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("love", value);
            }
        });

        //创建redis的配置
        FlinkJedisPoolConfig node01 = new FlinkJedisPoolConfig.Builder().setHost("node01").setPort(6379).build();

        //创建redissink
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(node01, new MyRedisMapper());
        l_words.addSink(redisSink);
        env.execute("SinkForRedisDemo");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String,String>> {


        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        /**
         * 表示从接收的数据中获取需要操作的redis key
         * @return
         */
        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data._1;
        }

        /**
         * 表示从接收的数据中获取需要操作的redis value
         * @param
         * @return
         */
        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data._2;
        }
    }
}
