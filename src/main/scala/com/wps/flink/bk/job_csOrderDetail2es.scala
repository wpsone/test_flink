package com.wps.flink.bk

import akka.io.Tcp.Event
import com.google.gson.JsonParser
import com.wps.flink.TXDeserialization
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import com.google.gson.JsonObject
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.http.HttpHost

import java.util
import scala.collection.mutable.ListBuffer

object job_csOrderDetail2es {

  val kafkaProdBrokers = "10.8.30.42:9092,10.8.30.43:9092,10.8.30.44:9092"
  val consumerGroup = "dabaostreaming.cs.cs_order_detail"

  val topic = "d.streamoffset.c9.cs_order_detail"

  val esIndex = "changestream-cs_order_detailprod"

  val parser = new JsonParser()

  def getLongDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

  def executeStream(seenv: StreamExecutionEnvironment,brokers:String,groupId:String,topic:String):DataStreamSink[JsonObject] = {
    val properties = new Properties()
    properties.setProperty("group.id",groupId)
    properties.setProperty("bootstrap.servers",brokers)
    val consumer = new FlinkKafkaConsumer010(topic, new TXDeserialization(), properties)

    import org.apache.flink.streaming.api.scala._
    seenv.addSource(consumer ).map(row=>{
      new String(row)
    }).flatMap{
      row => {
        val result = ListBuffer[JsonObject]()
//        val jsonParser = new JsonParser()
        val json: JsonObject = parser.parse(row).getAsJsonObject
        if (json!=null) {
          result.+=(json)
        }
        result
      }
    }

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("10.8.32.65",9200,"http"))
    httpHosts.add(new HttpHost("10.8.32.66",9200,"http"))
    httpHosts.add(new HttpHost("10.8.56.70",9200,"http"))

    new ElasticsearchSink.Builder[JsonObject](
      httpHosts,
      new ElasticsearchSinkFunction[JsonObject  ]{
        override def process(event: JsonObject, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val opType: String = event.get("operationType").toString
          val _id: String = event.get("_id").toString
          val typeMap = new util.HashMap[String, String]()
          typeMap.put("orderTime","date")
          typeMap.put("accessTime","date")
          typeMap.put("provideSiteId","date")
          typeMap.put("desSiteId","long")
          typeMap.put("startSendSiteId","long")
          typeMap.put("lastOptTypeId","long")
          typeMap.put("lastOptSiteId","long")
          typeMap.put("lastOptTime","date")
          typeMap.put("agingDetailList","array")
          typeMap.put("version","long")
          typeMap.put("createdTime","date")
          typeMap.put("updatedTime","date")
          typeMap.put("_class","remove")
          typeMap.put("recTime","date")
          typeMap.put("recSiteId","long")
          typeMap.put("agingRouteId","long")
          typeMap.put("deleteSignFlag","boolean")
          typeMap.put("lostFlag","boolean")
          typeMap.put("damagedFlag","boolean")
          typeMap.put("problemFlag","boolean")
          typeMap.put("eleContactFlag","boolean")
          typeMap.put("customerComplaintFlag","boolean")
          typeMap.put("wareHousingFlag","long")
          typeMap.put("warehousingTime","date")
          typeMap.put("returnFlag","boolean")
          typeMap.put("returnTime","date")
          typeMap.put("startCenterWgt","double")

          val jsonObject2t: String = event.get("fullDocument").toString
          opType match {
            case "\"replace\"" => {

            }
          }
        }
      }
    )
    null
  }

  def execute(seenv:StreamExecutionEnvironment):JobExecutionResult = {
    executeStream(seenv,kafkaProdBrokers,consumerGroup,topic)
    seenv.execute("kafka to es")
  }

  def parseDate(dateStr:String):Date = {
    null
  }
}
