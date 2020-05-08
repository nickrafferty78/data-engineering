

package spark.streaming.udemy

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.spark.streaming.StreamingContext
import org.apache.log4j.Level
import Utilities._
import org.apache.spark.sql.SparkSession
import spark.streaming.udemy.MyStreaming.jobName

/** Simple application to listen to a stream of Tweets and print them out */
object PrintTweets {
 
  /** Our main function where the action happens */
  def main(args: Array[String]) {

    // Configure Twitter credentials using twitter.txt
    setupTwitter()
    
    // Set up a Spark streaming context named "PrintTweets" that runs locally using
    // all CPU cores and one-second batches of data
    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(1))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    wordCounts.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
    
    // Get rid of log spam (should be called after the context is set up)
  //  setupLogging()

    // Create a DStream from Twitter using our streaming context
//    val tweets = TwitterUtils.createStream(ssc, None)
//
//    // Now extract the text of each status update into RDD's using map()
//    val statuses = tweets.map(status => status.getText())
//
//    // Print out the first ten
//    statuses.print()
//
//    // Kick it all off
//    ssc.start()
//    ssc.awaitTermination()
  }
}