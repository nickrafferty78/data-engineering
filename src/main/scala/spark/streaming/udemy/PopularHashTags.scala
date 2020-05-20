package spark.streaming.udemy
import Utilities._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

object PopularHashTags {

  def main(args: Array[String]): Unit = {


    setupTwitter()

    val ssc = new StreamingContext(new SparkConf().setAppName("PopularHashtags").setMaster("local[*]"), Seconds(1))

    setupLogging()

    val statuses = TwitterUtils.createStream(ssc, None)

    val tweets = statuses.map(status => status.getText)

    val eachWord = tweets.flatMap(tweet => tweet.split(" "))

    //val filterHashtags = eachWord.filter(word => word.startsWith("#"))

    val keyValuePair = eachWord.map(eachHashTag => (eachHashTag, 1))

    val reducedTotals = keyValuePair.reduceByKeyAndWindow((x,y) => x+y,Seconds(300))

    val sortedResults = reducedTotals.transform(rdd => rdd.sortBy(x => x._2, false))

    sortedResults.print(10)

    ssc.start()
    ssc.awaitTermination()

  }
}
