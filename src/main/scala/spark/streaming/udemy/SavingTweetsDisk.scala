package spark.streaming.udemy

import Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.TwitterStream;

object SavingTweetsDisk {

  def main(args: Array[String]): Unit = {


  setupTwitter()

  val ssc = new StreamingContext(new SparkConf().setAppName("Spark Save to Disk").setMaster("local[*]"),Seconds(1))

  setupLogging()


  val twitter = TwitterUtils.createStream(ssc, None)

  val statuses = twitter.flatMap(text => text.getText())

  var totalTweets: Long = 0

  statuses.foreachRDD((rdd, time)=>{
    if(rdd.count()>0){
      //need to collect all the data to one partition
      val repartitionedRDD = rdd.repartition(1).cache()
      repartitionedRDD.saveAsTextFile("Tweets" + time.milliseconds.toString())
      totalTweets += repartitionedRDD.count()
      println("Tweet count: " + totalTweets)
      if(totalTweets>1000){
        System.exit(0)
      }
    }
  })

  ssc.start()
  ssc.awaitTermination()

}
}
