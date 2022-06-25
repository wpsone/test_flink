package com.wps.flink.oggbean

import java.util.HashMap

trait Modle {
  /**
   * 定义需要从Kafka Ogg中抽取的字段及其对应的数据类型
   * @return
   */
  def getAimClumnMap: HashMap[String,String]

  /**
   * ES中作为_id的字段
   * @return
   */
  def getPrimaryKey:String
}
