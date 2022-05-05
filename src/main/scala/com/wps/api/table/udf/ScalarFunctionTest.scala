package com.wps.api.table.udf

import com.wps.api.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row

object ScalarFunctionTest {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
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

    //使用自定义的hash函数，求id的哈希值
    val hashCode = new HashCode(1.23)

    //Table API 调用方式
    val resultTable: Table = sensorTable.select('id, 'ts, hashCode('id))

    //SQL调用方式，首先要注册表和函数
    tableEnv.createTemporaryView("sensor",sensorTable)
    tableEnv.registerFunction("hashCode",hashCode)

    tableEnv.sqlQuery(
      """
        |select id,ts,hashCode(id)
        |from sensor
        |""".stripMargin)

    //转换成流打印输出
    resultTable.toAppendStream[Row].print("result")
    resultTable.toAppendStream[Row].print("sql")

    env.execute("scalar UDF test job")
  }

  //自定义一个求hash code的标量函数
  class HashCode(factor: Double) extends ScalarFunction {
    def eval(value:String):Int = {
      (value.hashCode*factor).toInt
    }
  }

}
