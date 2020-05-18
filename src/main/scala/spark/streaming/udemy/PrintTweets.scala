

package spark.streaming.udemy

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Level
import Utilities._
import org.apache.spark.sql.SparkSession
import spark.streaming.udemy.MyStreaming.jobName

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    setupTwitter()
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val conf = new SparkConf().setMaster("local[*]").setAppName("PrintTweets")
    val ssc = new StreamingContext(conf, Seconds(1))
    
    // Get rid of log spam (should be called after the context is set up)
    setupLogging()

    // Create a DStream from Twitter using our streaming context
    val tweets = TwitterUtils.createStream(ssc, None)

    // Now extract the text of each status update into RDD's using map()
    val statuses = tweets.map(status => status.getText())

    // Print out the first ten
    statuses.print()

    // Kick it all off
    ssc.start()
    ssc.awaitTermination()
  }
}