package spark.streaming.udemy

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.regex.{Matcher, Pattern}
import java.util.Locale

import org.apache.spark.sql._
import org.apache.spark._
import org.apache.spark.sql.functions._



object StructuredStreaming {

  case class AccessLog(ip:String, client:String, user:String, dateTime:String, request:String, status:String, bytes:String, referer:String, agent:String)


    val pattern = Utilities.apacheLogPattern()
    val datePattern = Pattern.compile("\\[(.*?) .+]")

    def parseDateField(field:String): Option[String] = {
      val dateMatcher = datePattern.matcher(field)
      if(dateMatcher.find()){
        val dateString = dateMatcher.group(1)
        val dateFormat= new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.ENGLISH)
        val date = (dateFormat.parse(dateString))
        val timestamp = new Timestamp(date.getTime);
        return Option(timestamp.toString)
      }
      else {
        None
      }
    }
    def parseLog(x:Row): Option[AccessLog] ={
      val matcher:Matcher = pattern.matcher(x.getString(0))
      if(matcher.matches()){
        val timeString = matcher.group(4)
        return Some(AccessLog(
          matcher.group(1),
          matcher.group(2),
          matcher.group(3),
          parseDateField(matcher.group(4)).getOrElse(""),
          matcher.group(5),
          matcher.group(6),
          matcher.group(7),
          matcher.group(8),
          matcher.group(9)
        ))
      }else {
        return None
      }
    }
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("StructuredStreaming")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation","/Users/nickrafferty/DataEngineering/spark-hello-world/src/main/scala/CheckpointFileHolderTemp")
      .getOrCreate()
    Utilities.setupLogging()
    val rawData = spark.readStream.text("logs")

    import spark.implicits._
    val structuredData = rawData.flatMap(parseLog).select("status", "dateTime")
    val windowed = structuredData.groupBy($"status", window($"dateTime", "1 hour")).count().orderBy("window")
    val query = windowed.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()
    spark.stop()
  }

}
