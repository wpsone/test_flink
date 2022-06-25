package com.wps.flink.sources

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import com.google.gson.{JsonObject, JsonParser}
import com.wps.flink.TXDeserialization
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010

import java.util.Properties

object KafkaConsumerSource {

  val kafkaBrokers="10.30.4.255:9092"
  val kafkaConsumerGroup = "ogg2es_test"
  val kafkaTopic = "ogg.mdfj.ass_bill_sort_analysis"

  def kafkaConsumer(environment: StreamExecutionEnvironment): DataStream[JsonObject] ={
    val properties = new Properties()
    properties.setProperty("group.id",kafkaConsumerGroup)
    properties.setProperty("bootstrap.servers",kafkaBrokers)
    val consumer = new FlinkKafkaConsumer010(kafkaTopic,new TXDeserialization(),properties)
    consumer.setStartFromEarliest()

    //将消费的消息由默认的DataStream[Array[Byte]] 封装成DataStream[JsonObject]
    import org.apache.flink.streaming.api.scala._
    val streamFromKafka:DataStream[JsonObject] = environment
      .addSource(consumer)
      .map(
        row=>{
          val result = new String(row)
          val jsonParser = new JsonParser()
          val json = jsonParser.parse(result).getAsJsonObject
          json
        }
      )
    streamFromKafka
  }

}
