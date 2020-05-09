package org.forsynet.mqttgpio

import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object MqttSparkWindow {

  def mqttWindows()= {
    val iotLogStatusDF = MqttGpioSpark.readMqttLogs()

    iotLogStatusDF
      .withWatermark("iottimestamp","2 minute")
      .groupBy(col("status"), window(col("iottimestamp"), "1 minute").as("time_window"))
      .agg(count("status").as("Status_Count"))
      .select(
        col("time_window").getField("start").as("Start"),
        col("time_window").getField("end").as("End"),
        col("status").as("Status"),
        col("Status_Count")
      )
//      .orderBy(col("Start").desc)

  }

  def writeToConsole(): Unit ={

      mqttWindows().writeStream
      .format("console")
      .outputMode(OutputMode.Append)
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    writeToConsole()
  }

}
