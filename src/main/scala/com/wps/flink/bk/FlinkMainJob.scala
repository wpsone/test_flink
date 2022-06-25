package com.wps.flink.bk

import com.wps.flink.sources.KafkaConsumerSource
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.util.concurrent.TimeUnit

object FlinkMainJob extends App {

  val seenv = StreamExecutionEnvironment.getExecutionEnvironment
  seenv.setRestartStrategy(RestartStrategies.failureRateRestart(3,// 每个测量时间间隔最大失败次数
    Time.of(5,TimeUnit.MINUTES) ,//失败率测量的时间间隔
    Time.of(10,TimeUnit.SECONDS) // 两次连续重启尝试的时间间隔
  ))

  seenv.setBufferTimeout(-1)
  seenv.setParallelism(2)

  seenv.getCheckpointConfig.setCheckpointTimeout(3000001)
  seenv.getCheckpointConfig.setCheckpointInterval(10001)
  seenv.getCheckpointConfig.setMaxConcurrentCheckpoints(6)
  seenv.setStateBackend(new MemoryStateBackend(100*1024*1024,true))
  println("----------------")
  KafkaConsumerSource.kafkaConsumer(seenv)
  seenv.execute("ConsumerOggTest")
}
