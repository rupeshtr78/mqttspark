package org.forsynet.mqttgpio

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.forsynet.mqttgpio.MqttGpioSpark.spark



object MqttCassandraSink {


  spark.conf.set("spark.cassandra.connection.host","192.168.1.200")
  spark.conf.set("spark.cassandra.connection.port","9042")


  val mqttWindowDF = MqttSparkWindow.mqttWindows()

//  mqttWindow.createCassandraTable("hyper","mqtt")

  def writeToCassandraForEach(): Unit = {

   mqttWindowDF
     .writeStream
     .foreachBatch { (batchDF: DataFrame, batchID: Long) =>
       batchDF.write
         .format("org.apache.spark.sql.cassandra")
         .options(Map("table" -> "mqtt", "keyspace" -> "hyper"))
         .mode(SaveMode.Append)
         .save
     }
     .start()
     .awaitTermination()
   }


  def main(args: Array[String]): Unit = {
//    MqttSparkWindow.writeToConsole()
    writeToCassandraForEach()

  }
}
