package org.forsynet.mqttgpio

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse
import org.apache.spark.sql.cassandra
import org.apache.spark.sql.functions._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.cql._
import com.datastax.spark.connector._


object MqttGpioSpark {

  val spark = SparkSession.builder()
    .appName("Mqtt Gpio Spark")
    .config("spark.cassandra.connection.host","192.168.1.200")
    .config("spark.cassandra.connection.port","9042")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

//  org.apache.spark.sql.mqtt.HDFSMQTTSourceProvider
//  org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider


  def readMqttLogs() = {
   val lines = spark.readStream
      .format("org.apache.bahir.sql.streaming.mqtt.MQTTStreamSourceProvider")
      .option("topic", "rupesh/gpiotopic")
      .option("persistence", "memory")
      .option("cleanSession", "true")
      .load("tcp://192.168.1.200:8883")
      .selectExpr("CAST(payload AS STRING)").as[String]


    val logsDs = lines.map {
      row =>
        implicit val format = DefaultFormats
        parse(row).extract[PayLoadClass]
    }

    logsDs.
        select(col("iottimestamp").cast("timestamp")
          ,col("accesslogs"))
       .withColumn("user", parseLog("user")($"accesslogs"))
       .withColumn("dateTime", parseLog("datetimeNoBrackets")($"accesslogs"))
       .withColumn("request", parseLog("request")($"accesslogs"))
       .withColumn("agent", parseLog("agent")($"accesslogs"))
       .withColumn("status", parseLog("status")($"agent"))



  }

  def writeMqtt(): Unit ={
  readMqttLogs()
    .writeStream
    .format("console")
    .outputMode("append")
    .start()
    .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    writeMqtt()

//    readMqttLogs()
  }


}
