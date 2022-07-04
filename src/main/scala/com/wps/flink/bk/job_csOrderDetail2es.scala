package com.wps.flink.bk

import com.google.gson.{JsonElement, JsonObject, JsonParser}
import com.wps.flink.TXDeserialization
import org.apache.flink.api.common.JobExecutionResult
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

import java.text.SimpleDateFormat
import java.util.{Date, Properties}
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.client.Requests

import java.util
import scala.collection.JavaConversions._
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
    val stream: DataStream[JsonObject] = seenv.addSource(consumer).map(row => {
      new String(row)
    }).flatMap {
      row => {
        val result = ListBuffer[JsonObject]()
        //        val jsonParser = new JsonParser()
        val json: JsonObject = parser.parse(row).getAsJsonObject
        if (json != null) {
          result.+=(json)
        }
        result
      }
    }

    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost("10.8.32.65",9200,"http"))
    httpHosts.add(new HttpHost("10.8.32.66",9200,"http"))
    httpHosts.add(new HttpHost("10.8.56.70",9200,"http"))

    val esSinkBuilder = new ElasticsearchSink.Builder[JsonObject](
      httpHosts,
      new ElasticsearchSinkFunction[JsonObject] {
        override def process(event: JsonObject, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val opType: String = event.get("operationType").toString
          val _id: String = event.get("_id").toString
          val typeMap = new util.HashMap[String, String]()
          typeMap.put("orderTime", "date")
          typeMap.put("accessTime", "date")
          typeMap.put("provideSiteId", "date")
          typeMap.put("desSiteId", "long")
          typeMap.put("startSendSiteId", "long")
          typeMap.put("lastOptTypeId", "long")
          typeMap.put("lastOptSiteId", "long")
          typeMap.put("lastOptTime", "date")
          typeMap.put("agingDetailList", "array")
          typeMap.put("version", "long")
          typeMap.put("createdTime", "date")
          typeMap.put("updatedTime", "date")
          typeMap.put("_class", "remove")
          typeMap.put("recTime", "date")
          typeMap.put("recSiteId", "long")
          typeMap.put("agingRouteId", "long")
          typeMap.put("deleteSignFlag", "boolean")
          typeMap.put("lostFlag", "boolean")
          typeMap.put("damagedFlag", "boolean")
          typeMap.put("problemFlag", "boolean")
          typeMap.put("eleContactFlag", "boolean")
          typeMap.put("customerComplaintFlag", "boolean")
          typeMap.put("wareHousingFlag", "long")
          typeMap.put("warehousingTime", "date")
          typeMap.put("returnFlag", "boolean")
          typeMap.put("returnTime", "date")
          typeMap.put("startCenterWgt", "double")

          val jsonObject2t: String = event.get("fullDocument").toString
          opType match {
            case "\"replace\"" => {
              val jsonObject = JsonObjectToHashMap(event.get("fullDocument").getAsJsonObject, typeMap)
              jsonObject.foreach {
                case s => {
                  val key = s.get("orderagingkey")
                  if (key != null)
                    requestIndexer.add(Requests.indexRequest().index(esIndex).`type`("_doc").id(key.asInstanceOf[String]).source(s))
                }
              }
            }
            case "\"update\"" => {
              val jsonObject = JsonObjectToHashMap(event.get("fullDocument").getAsJsonObject, typeMap)
              jsonObject.foreach {
                case s => {
                  val key = s.get("orderagingkey")
                  if (key != null)
                    requestIndexer.add(Requests.indexRequest().index(esIndex).`type`("_doc").id(key.asInstanceOf[String]).source(s))
                }
              }
            }
            case "\"delete\"" => {

            }
            case _ => null.asInstanceOf[JsonObject]
          }
        }
      }
    )

    val failureHandler = new ActionRequestFailureHandler {
      //数据格式异常情况，直接丢掉
      override def onFailure(actionRequest: ActionRequest, throwable: Throwable, i: Int, requestIndexer: RequestIndexer): Unit = {
        println("ex:"+throwable.getMessage)
      }
    }
    esSinkBuilder.setFailureHandler(failureHandler)
    esSinkBuilder.setBulkFlushMaxActions(50)

    stream.name("写es").addSink(esSinkBuilder.build()).name("log写es")
  }

  def JsonObjectToHashMap(jsonObject: JsonObject,typeMap: java.util.Map[String,String]): util.ArrayList[util.HashMap[String,Object]]={
    val data = new util.HashMap[String, Object]
    val list = new util.ArrayList[util.HashMap[String, Object]]
    val subList = new util.ArrayList[util.HashMap[String, Object]]
    val iter = jsonObject.entrySet().iterator()
    while (iter.hasNext) {
      val k: String = iter.next().getKey
      typeMap.get(k) match {
        case "date" => {
          val field = if ((jsonObject.get(k )!=null) && (!jsonObject.get(k).isJsonNull)) jsonObject.get(k).getAsString.replaceAll("\"","" ) else ""
          data.put(k.toLowerCase(),parseDate(field).asInstanceOf[Object])
        }
        case "long" => {
          val field: Double = parseLong(jsonObject, k)
          data.put(k.toLowerCase(),field.asInstanceOf[Object])
        }
        case "double" => {
          val field = if ((jsonObject.get(k )!=null) && (!jsonObject.get(k).isJsonNull && jsonObject.get(k  ).isJsonPrimitive))
            jsonObject.get(k  ).getAsJsonPrimitive.getAsNumber.doubleValue()
          else 0L
          data.put(k.toLowerCase(),field.asInstanceOf[Object])
        }
        case "boolean" => {
          val field = if ((jsonObject.get(k )!=null) && (!jsonObject.get(k).isJsonNull && jsonObject.get(k).isJsonPrimitive))
            jsonObject.get(k).getAsJsonPrimitive.getAsBoolean
          else false
          data.put(k.toLowerCase(),field.asInstanceOf[Object])
        }
        case "remove"=>{}
        case "array"=>{
          val typeMap = new util.HashMap[String,String]()

          typeMap.put("projectId","long")
          typeMap.put("projectType","long")
          typeMap.put("projectSubscribeId","long")
          typeMap.put("provideSiteId","long")
          typeMap.put("agingId","long")
          typeMap.put("agingConfigId","long")
          typeMap.put("agingStartFlag","boolean")
          typeMap.put("agingFinishFlag","boolean")
          typeMap.put("agingAutoFinishFlag","boolean")
          typeMap.put("overTimeFlag","boolean")
          typeMap.put("influenceAgingFlag","boolean")
          if ((jsonObject.get(k ) != null) && (!jsonObject.get(k  ).isJsonNull && jsonObject.get(k).isJsonArray)) {
            jsonObject.get(k  ).getAsJsonArray.foreach{
              s=>{
                if (s.isInstanceOf[JsonObject]) {
                  val stringToObjects = JsonObjectToHashMap(s.asInstanceOf[JsonObject], typeMap)
                  if (!stringToObjects.isEmpty) subList.add(JsonObjectToHashMap(s.asInstanceOf[JsonObject],typeMap)(0))
                }
              }
            }
          }
        }
        case null => {
          val field = if ((jsonObject.get(k )!=null) && (!jsonObject.get(k  ).isJsonNull)) jsonObject.get(k).getAsString.replaceAll("\"","" ) else ""
          if ("_id".equals(k    )) {
            data.put("billcode",field.asInstanceOf[Object])
          } else {
            data.put(k.toLowerCase( ),field.asInstanceOf[Object])
          }
        }

      }
    }
    if (subList.isEmpty) {
      list.add(data)
    } else {
      subList.foreach{
        s=>{
          val re: util.HashMap[String, Object] = data.clone().asInstanceOf[util.HashMap[String, Object]]
          re.putAll(s)
          list.add(re)
        }
      }
    }
    list
  }

  def parseLong(jsonObj:JsonObject,k:String):Double = {
    var result = 0L
    if ((jsonObj.get(k  )!=null)&&(!jsonObj.get(k).isJsonNull && jsonObj.get(k).isJsonPrimitive)) {
      result = jsonObj.get(k  ).getAsJsonPrimitive.getAsNumber.doubleValue().toLong
    }
    result
  }

  def execute(seenv:StreamExecutionEnvironment):JobExecutionResult = {
    executeStream(seenv,kafkaProdBrokers,consumerGroup,topic)
    seenv.execute("kafka to es")
  }

  def parseDate(dateStr:String):Date = {
    null
  }
}
