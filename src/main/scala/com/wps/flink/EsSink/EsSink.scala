package com.wps.flink.EsSink

import com.google.gson.internal.LazilyParsedNumber
import com.google.gson.{JsonObject, JsonParser}
import com.wps.flink.oggbean.Modle
import com.wps.flink.utils.JsonUtils
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ActionRequestFailureHandler, ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.ActionRequest
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.client.Requests
import org.elasticsearch.common.xcontent.XContentFactory
import org.omg.CORBA.Environment

import java.util

object EsSink {

  val ES_INDEX = "ogg2es_test2|"
  val ES_HOST = "10.30.4.255"
  val ES_PORT = 9200
  val ES_SCHEMA = "http"
  val ES_TYPE = "_doc"
  val parser = new JsonParser()

  def createAndInitEsSinkBuilder(environment:StreamExecutionEnvironment,modle: Modle) : ElasticsearchSink.Builder[JsonObject] = {
    //创建 esSinkBuilder
    val esSinkBuilder: ElasticsearchSink.Builder[JsonObject] = createEsSinkBuilder(modle)
    //创建 failureHandler
    val failureHandler = new ActionRequestFailureHandler {
      override def onFailure(actionRequest: ActionRequest, throwable: Throwable, i: Int, requestIndexer: RequestIndexer): Unit = {
        println("ex:"+throwable.getMessage)
      }
    }
    esSinkBuilder.setFailureHandler(failureHandler)
//    esSinkBuilder.setBulkFlushBackoff(true)
    //刷新前缓冲的最大动作量
    esSinkBuilder.setBulkFlushMaxActions(10)
    //刷新前缓冲区的最大数据大小(以MB为单位)
//    esSinkBuilder.setBulkFlushMaxSizeMb(500)
    //缓冲操作的数量或大小如何都要刷新的时间间隔
    esSinkBuilder.setBulkFlushInterval(5000)
    esSinkBuilder
  }

  def createEsSinkBuilder(modle: Modle):ElasticsearchSink.Builder[JsonObject] = {
    val httpHosts = new util.ArrayList[HttpHost]
    httpHosts.add(new HttpHost(ES_HOST,ES_PORT,ES_SCHEMA))
    val esSinkBuilder: ElasticsearchSink.Builder[JsonObject] = new ElasticsearchSink.Builder[JsonObject](
      httpHosts,
      new ElasticsearchSinkFunction[JsonObject] {

        /**
         * 对每条数据进行处理，根据op_type 类型进行处理
         * @param t
         * @param runtimeContext
         * @param requestIndexer
         */
        override def process(event: JsonObject, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          val opType: String = event.get("op_type").toString
          opType match {
            case "\"U\"" => {
              val afterStr: JsonObject = event.get("after").getAsJsonObject
              requestIndexer.add(updateDocRequest(afterStr,modle))
            }
            case "\"I\"" => {
              val afterStr: JsonObject = event.get("after").getAsJsonObject
              requestIndexer.add(updateDocRequest(afterStr,modle))
            }
            case "\"D\"" =>
              val beforeStr: JsonObject = event.get("before").getAsJsonObject
              requestIndexer.add(deleteDocRequest(beforeStr,modle))
            case _ => null.asInstanceOf[JsonObject]
          }
        }

        //todo 更新指定字段
        def updateRequest(element: JsonObject,modle: Modle): UpdateRequest = {
          val key = element.get("ID"  ).getAsString
          val billCode = "33333"
          val updateRequest = new UpdateRequest()
          updateRequest
            .index(ES_INDEX)
            .`type`(ES_TYPE)
            .id(key.asInstanceOf[String])
            .doc(XContentFactory.jsonBuilder.startObject.field("bill_code",billCode ).endObject)
          updateRequest
        }

        //todo 删除记录
        def deleteDocRequest(element:JsonObject,modle: Modle):DeleteRequest = {
          val keyValue: String = element.get(modle.getPrimaryKey).getAsString
          val deleteRequest = new DeleteRequest()
          deleteRequest.index(ES_INDEX)
            .`type`(ES_TYPE)
            .id(keyValue.asInstanceOf[String])
          deleteRequest
        }

        //todo 根据_id更新整个DOC
        def updateDocRequest(element: JsonObject,modle: Modle):IndexRequest = {
          val stringToObject: util.HashMap[String, Object] = JsonUtils.jsonObject2HashMap(element, modle.getAimClumnMap)
          val key: String = element.get(modle.getPrimaryKey).toString
          Requests.indexRequest()
            .index(ES_INDEX)
            .`type`(ES_TYPE)
            .id(key.asInstanceOf[String])
            .source(stringToObject)
        }
      }
    )
    esSinkBuilder
  }

  def main(args: Array[String]): Unit = {
    println(new LazilyParsedNumber("1222" ).doubleValue())
  }
}
