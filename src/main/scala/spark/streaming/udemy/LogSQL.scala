package spark.streaming.udemy

import java.util.regex.Matcher

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel


object LogSQL {

  case class Record(url: String, status: Int, agent:String)
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("LogSQL")
    val ssc = new StreamingContext(conf, Seconds(1))

    setupLogging()

    val pattern = apacheLogPattern()

    val lines = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK)

    val requests = lines.map(x => {
      val matcher:Matcher = pattern.matcher(x)
      if(matcher.matches()){
        val request = matcher.group(5)
        val requestFields = request.toString().split(" ")
        val url = util.Try(requestFields(1)) getOrElse "[error]"
        (url, matcher.group(6).toInt, matcher.group(5))
      }else{
        ("error", 0, "error")
      }
    })

    //data frames
    requests.foreachRDD((rdd, time) => {
      val spark = SparkSession.builder().appName("LogSQL").getOrCreate()

      import spark.implicits._

      val requestsDataFrame = rdd.map(w => Record(w._1, w._2, w._3 )).toDF()

      requestsDataFrame.createOrReplaceTempView("requests")

      val wordCountsDataFrame = spark.sqlContext.sql("select agent, count(*) as total from requests group by agent")
      println(s"=======$time=======")
      wordCountsDataFrame.show()

    })
    ssc.checkpoint("/Users/nickrafferty/DataEngineering/spark-hello-world/src/main/scala/CheckpointFileHolderTemp")
    ssc.start()
    ssc.awaitTermination()
  }

}
