package com.wps.api.table

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, Kafka, Schema}

object KafkaPipeline {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //1. 创建表执行环境
    val tableEnv = StreamTableEnvironment.create(env)

    //2. 定义到kafka的连接，创建输入表
    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sensor")
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("timestamp",DataTypes.BIGINT())
        .field("temperature",DataTypes.DOUBLE())
        .field("pt",DataTypes.TIMESTAMP(3))
        .proctime()
      )
      .createTemporaryTable("kafkaInputTable")

    //3. 表的查询转换
    val sensorTable: Table = tableEnv.from("kafkaInputTable")
    //3.1 简单查询转换
    val resultTable: Table = sensorTable
      .select('id,'temperature)
      .filter('id=== "sensor_1")
    //3.2 聚合转换
    val aggResultTable: Table = sensorTable
      .groupBy('id)
      .select('id,'id.count as 'count)

    tableEnv.connect(new Kafka()
      .version("0.11")
      .topic("sinkTest")
      .property("zookeeper.connect","localhost:2181")
      .property("bootstrap.servers","localhost:9092")
    )
      .withFormat(new Csv())
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.DOUBLE())
      )
      .createTemporaryTable("kafkaOutputTable")

    resultTable.insertInto("kafkaOutputTable")
//    aggResultTable.insertInto("kafkaOutputTable")

    env.execute("kafka pipeline test job")
  }

}
