package spark.streaming.udemy

import java.util.concurrent.atomic.AtomicLong

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import spark.streaming.udemy.Utilities.setupTwitter

object AverageTweetLength {

  def main(args: Array[String]): Unit = {


    setupTwitter()

    val ssc = new StreamingContext(new SparkConf().setMaster("local[*]").setAppName("AverageTweetLength"),Seconds(1))

    val tweets = TwitterUtils.createStream(ssc,None)

    val statuses = tweets.map(tweet => tweet.getText())

    val length = statuses.map(statusText=> statusText.length())

    val totalChars = new AtomicLong(0)
    var maxLength = new AtomicLong(0)



    length.foreachRDD(rdd =>{
      var count = rdd.count()
      if(count>0){
        totalChars.getAndAdd(rdd.reduce((x,y)=> x+y))

      if(totalChars.get() > maxLength.get()){
        maxLength = totalChars
      }
        println("Max Length: " + maxLength)
      }
    })

    ssc.start()
    ssc.awaitTermination()

  }
}
