package spark.streaming.udemy
import java.util.regex.Matcher

import Utilities._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}

object LogParser {

  def main(args: Array[String]): Unit = {

    val ssc = new StreamingContext("local[*]", "LogParser", Seconds(1))

    setupLogging()

    val pattern = apacheLogPattern()

    val lines = ssc.socketTextStream("localhost", 9999)

    val requests = lines.map(x => {val matcher:Matcher = pattern.matcher(x); if (matcher.matches()) matcher.group(5) })

    //GET /page-sitemap.xml HTTP/1.0
    //URLs
    val urls = requests.map(request => { val arr = request.toString.split(" "); if (arr.length == 3) arr(1) else "error!"})

    val urlCounts = urls.map(x=> (x,1)).reduceByKeyAndWindow((x,y) => x+y, (x,y) => x-y, Seconds(300), Seconds(1))

    val sorted = urlCounts.transform(rdd => rdd.sortBy(x => x._2, false))

    sorted.print(10)


    ssc.checkpoint("/Users/nickrafferty/DataEngineering/spark-hello-world/src/main/scala/spark/streaming/udemy")
    ssc.start()
    ssc.awaitTermination()

  }


}
