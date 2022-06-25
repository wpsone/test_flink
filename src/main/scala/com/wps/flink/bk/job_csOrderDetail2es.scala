package com.wps.flink.bk

import com.google.gson.JsonParser
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.runtime.client.JobExecutionException
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.text.SimpleDateFormat
import scala.util.parsing.json.JSONObject

object job_csOrderDetail2es {

  val kafkaProdBrokers = "10.8.30.42:9092,10.8.30.43:9092,10.8.30.44:9092"
  val consumerGroup = "dabaostreaming.cs.cs_order_detail"

  val topic = "d.streamoffset.c9.cs_order_detail"

  val esIndex = "changestream-cs_order_detailprod"

  val parser = new JsonParser()

  def getLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def executeStream(seenv: StreamExecutionEnvironment,brokers:String,groupId:String,topic:String):DataStreamSink[JSONObject] = {
    null
  }

  def execute(seenv:StreamExecutionEnvironment):JobExecutionResult = {
    executeStream(seenv,kafkaProdBrokers,consumerGroup,topic)
    seenv.execute("kafka to es")
  }

}
