package spark.streaming.udemy
import Utilities._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogParser {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()

    val pattern = apacheLogPattern()


  }


}
