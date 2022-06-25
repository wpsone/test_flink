package com.wps.flink.utils

import com.google.gson.JsonObject
import org.apache.kafka.common.protocol.types.Field.Str

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, HashMap, Map, TimeZone}

object JsonUtils {

  def main(args: Array[String]): Unit = {

  }

  def jsonObject2HashMap(jsonObj: JsonObject,
                           aimClumnMap:Map[String,String]): HashMap[String,Object] ={
    val data = new HashMap[String,Object]()
    //遍历aimClumnMap中字段
    val iter = aimClumnMap.entrySet().iterator()
    while (iter.hasNext) {
      val clumnName: String = iter.next().getKey
      val clumnType:String = aimClumnMap.get(clumnName  ).toLowerCase

      clumnType match {
        case "date" => {
          val fieldFromJson = parseDate(jsonObj,clumnName.toUpperCase)
          data.put(clumnName.toLowerCase(),fieldFromJson.asInstanceOf[Object])
        }
      }
    }
    data
  }

  def parseDate(jsonObject: JsonObject,key:String):Date = {
    var result =
      if ((jsonObject.get(key  )!=null) && (!jsonObject.get(key).isJsonNull))
        jsonObject.get(key  ).getAsString.replaceAll("\"","")
      else ""
      parseDate(result)

  }

  def parseDate(dateStr: String):Date = {
    if (dateStr==null || dateStr.equals()) return null.asInstanceOf[Date]
    val formatStr = {
      dateStr match {
        case str if (str.length == 7) => "yyyy-MM"
        case str if (str.length == 10) => "yyyy-MM-dd"
        case str if (!str.contains("T"  ) && str.length == 19) => "yyyy-MM-dd HH:mm:ss"
        case str if str.contains("T") && str.length == 19 => "yyyy-MM-dd'T'HH:mm:ss"
        case str if str.contains("."  ) && str.length==23 => "yyyy-MM-dd HH:mm:ss.SSS"
        case str if str.contains("."  ) && str.length == 29 => "yyyy-MM-dd HH:mm:ss.SSSSSS"
        case _=>"yyyy-MM-dd"
      }
    }
    val df = new SimpleDateFormat(formatStr)
    df.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"))
    df.parse(dateStr)
  }
}
