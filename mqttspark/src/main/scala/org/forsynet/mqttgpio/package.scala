package org.forsynet

import org.apache.spark.sql.types.{StringType, StructField, StructType}

package object mqttgpio {

  import org.apache.spark.sql.functions._

  val regexPatterns = Map(
    "ddd" -> "\\d{1,3}".r,
    "ip" -> """s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"""".r,
    "client" -> "(\\S+)".r,
    "user" -> "(\\S+)".r,
    "dateTime" -> "(\\[.+?\\])".r,
    "datetimeNoBrackets" -> "(?<=\\[).+?(?=\\])".r,
    "request" -> "\"(.*?)\"".r,
    "status" -> "(\\d{3})".r,
    "bytes" -> "(\\S+)".r,
    "referer" -> "\"(.*?)\"".r,
    "agent" -> """\"(.*)\"""".r
  )

  def parseLog(regExPattern: String) = udf((url: String) =>
    regexPatterns(regExPattern).findFirstIn(url) match
    {
      case Some(parsedValue) => parsedValue
      case None => "unknown"
    }
  )

  val payLoadSchema = StructType(Array(
        StructField("accesslog",StringType),
        StructField("iottimestamp" , StringType)

  ))

  case class PayLoadClass(
                           iottimestamp: String,
                           accesslogs:String

                         )


  case class MqttLogs(
                       iottimestamp:String,
                       user: String,
                       request: String,
                       agent: String,
                       status: String
                     )


}
