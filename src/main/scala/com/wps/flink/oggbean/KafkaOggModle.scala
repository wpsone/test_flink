package com.wps.flink.oggbean
import java.util


object KafkaOggModle extends Serializable with Modle {
  /**
   * 定义需要从Kafka Ogg中抽取的字段及其对应的数据类型
   *
   * @return
   */
  override def getAimClumnMap: util.HashMap[String, String] = {
    val typeMap = new util.HashMap[String,String]()
    typeMap.put("id","String")
    typeMap.put("lock_version","Long")
    typeMap.put("last_update_time","Date")
    typeMap.put("last_updator","String")
    typeMap.put("last_updator_name","String")
    typeMap.put("bill_code","String")
    typeMap.put("type","String")
    typeMap.put("order_time","Date")
    typeMap.put("province","String")
    typeMap.put("city","String")
    typeMap.put("city_code","String")
    typeMap.put("send_city","String")
    typeMap
  }

  /**
   * ES中作为_id的字段
   *
   * @return
   */
  override def getPrimaryKey: String = {
    val primaryKey:String = "ID"
    primaryKey
  }
}
