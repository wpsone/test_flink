package com.wps.api.table.udf

import com.wps.api.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, createTypeInformation}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.TableFunction
import org.apache.flink.types.Row

object TableFunction {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //创建表执行环境
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)

    val inputStream: DataStream[String] = env.readTextFile("E:\\IdeaProjects\\test_flink\\src\\main\\resources\\sensor.txt")

    //map成样例类类型
    val dataStream: DataStream[SensorReading] = inputStream.map(data => {
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp * 1000L
        }
      })

    //将流转换成表，直接定义时间字段
    val sensorTable: Table = tableEnv.fromDataStream(dataStream, 'id, 'temperature, 'timestamp.rowtime as 'ts)

    //先创建一个UDF对象
    val split = new Split("_")

    //Table API调用
    val resultTable: Table = sensorTable.joinLateral(split('id) as('word, 'length))
      .select('id, 'ts, 'word, 'length)

    //SQL调用
    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("split",split)
    val resultSqlTable: Table = tableEnv.sqlQuery(
      """
        |select id,ts,word,length
        |from
        |sensor,lateral table(split(id)) as splitid(word,length)
        |""".stripMargin)

    resultTable.toAppendStream[Row].print("result")
    resultSqlTable.toAppendStream[Row].print("sql")

    env.execute("table function test job")
  }

  //自定义TableFunction,实现分割字符串并统计长度(word,length)
  class Split(separator: String) extends TableFunction[(String,Int)] {
    def eval(str:String):Unit = {
      str.split(separator).foreach(
        word=>collect((word,word.length))
      )
    }
  }
}
