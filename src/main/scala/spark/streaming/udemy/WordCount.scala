package spark.streaming.udemy

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val input = sc.textFile("src/main/scala/spark/streaming/udemy/book.txt")

    val words = input.flatMap(line => line.split(' '))

    val lowerCaseWords = words.map(x => x.toLowerCase)

    val wordCounts = lowerCaseWords.countByValue()



    val sample = wordCounts.take(20)

    for((word,count)<- sample){
      println(word + " " + count)
    }
  }

}
