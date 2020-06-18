package spark.streaming.udemy

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import Utilities._
import java.util.regex.Matcher

object LogAlarms {

  def main(args: Array[String]): Unit = {


    val ssc = new StreamingContext("local[*]", "LogAlarm", Seconds(1))

    val stream = ssc.socketTextStream("localhost", 9999, StorageLevel.MEMORY_AND_DISK_SER)

    setupLogging()

    val pattern = apacheLogPattern()

    val statusCodes = stream.map(x => {
      val matcher: Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(6) else "[error]"
    })



    val successOrFailure = statusCodes.map(x =>
      {val statusCode = util.Try(x.toInt) getOrElse 0
      if (statusCode >= 200 && statusCode < 300){
        "Success"
      }else if (statusCode >= 500 && statusCode < 600){
        "Failure"
      }else{
        "Other"
      }
      })

    val countSuccessVsFailure = successOrFailure.countByValueAndWindow(Seconds(300), Seconds(1))



    countSuccessVsFailure.foreachRDD((rdd, time) => {
      var totalSucces:Long = 0
      var totalFailure: Long = 0

      if(rdd.count()>0){
        val elements = rdd.collect()
        for(element <- elements){
          val result = element._1
          val count = element._2
          if(result == "Sucess"){
            totalSucces += count
          }else if (result == "Failure"){
            totalFailure += count
          }
        }
      }

      print("Total Succes : " + totalSucces + " Total Failure : " + totalFailure)

      if(totalFailure + totalSucces > 100){
        val ratio: Double = util.Try(totalFailure.toDouble/totalSucces.toDouble) getOrElse 1.0
        if(ratio>0.5){
          println("Something is completely wrong")
        }else{
          "All systems go"
        }
      }

    })




    ssc.checkpoint("/Users/nickrafferty/DataEngineering/spark-hello-world/src/main/scala/spark/streaming/udemy")
    ssc.start()
    ssc.awaitTermination()
  }


}
