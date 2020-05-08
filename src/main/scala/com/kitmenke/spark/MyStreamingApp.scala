package com.kitmenke.spark

import hbase.HBase.logger
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, Trigger}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, SparkSession}




/**
 * Spark Structured Streaming app
 *
 * Takes one argument, for Kafka bootstrap servers (ex: localhost:9092)
 */

case class CustomerReviewWithJS(jsonData:CustomerReview)
case class CustomerReview(marketplace: String, customer_id: String, review_id: String, product_id: String, product_parent: Int, product_title: String, product_category: String, star_rating: Int, helpful_votes: Int, total_votes: Int, vine: String, verified_purchase: String, review_headline: String, review_body: String, review_date: java.sql.Timestamp)
case class HBaseCustomer(name: String, birthdate: String, mail: String, sex: String, username: String)
case class CombinedData(customerReview: CustomerReview, HBaseCustomer: HBaseCustomer)
//example
//{"marketplace":"US","customer_id":36442766,"review_id":"R221CXBGAJ0LYO","product_id":"B00HK3QOHS","product_parent":841863599,"product_title":"Estes 4615 Proto X Rotor Blade Replacement Props Set (4)","product_category":"Toys","star_rating":5,"helpful_votes":0,"total_votes":0,"vine":"N","verified_purchase":"Y","review_headline":"Might as well buy a couple of these.","review_body":"If you own a Proto X it is only a matter of one or two flights that you will need these. By the time you replace the blades four times I figure the copter will be roasted as the motors will have come loose from repeated strikes.<br /><br />Then just go buy another Proto X because they are a blast as well as inexpensive.","review_date":"2014-12-28T00:00:00.000-06:00"}
object MyStreamingApp {
  lazy val logger: Logger = Logger.getLogger(this.getClass)
  val jobName = "MyStreamingApp"
  val schema: StructType = new StructType()
    .add("marketplace", StringType, nullable = true)
    .add("customer_id", IntegerType, nullable = true)
    .add("review_id", StringType, nullable = true)
    .add("product_id", StringType, nullable = true)
    .add("product_parent", IntegerType, nullable = true)
    .add("product_title", StringType, nullable = true)
    .add("product_category", StringType, nullable = true)
    .add("star_rating", IntegerType, nullable = true)
    .add("helpful_votes", IntegerType, nullable = true)
    .add("total_votes", IntegerType, nullable = true)
    .add("vine", StringType, nullable = true)
    .add("verified_purchase", StringType, nullable = true)
    .add("review_headline", StringType, nullable = true)
    .add("review_body", StringType, nullable = true)
    .add("review_date", TimestampType, nullable = true)

  def main(args: Array[String]): Unit = {

    try {
      val spark = SparkSession.builder().config("spark.hadoop.dfs.client.use.datanode.hostname", "true").appName(jobName).master("local[*]").getOrCreate()
      val bootstrapServers = "35.208.65.122:9092,34.68.16.1:9092,35.225.151.65:9092"


      val df = spark
        .readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrapServers)
        .option("startingOffsets", "earliest")
        .option("subscribe", "reviews")
        .option("maxOffsetsPerTrigger", "200")
        .load()
        .selectExpr("CAST(value AS STRING)")

      df.printSchema()

      import spark.implicits._

      val out = compute(df)
     val formattedData = out.as[CustomerReviewWithJS].map(CustomerReviewWithJS => CustomerReviewWithJS.jsonData)


      //
      val result = formattedData.mapPartitions(partition => {
        val conf = HBaseConfiguration.create()
        conf.set("hbase.zookeeper.quorum", "35.184.255.239")
        val connection = ConnectionFactory.createConnection(conf)
        val table = connection.getTable(TableName.valueOf("nickrafferty78:users"))
        partition.map(customer => {
          val result = table.get(new Get(Bytes.toBytes(customer.customer_id)))
          val name = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("name")))
          val birthdate = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("birthdate")))
          val mail = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("mail")))
          val sex = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("sex")))
          val username = Bytes.toString(result.getValue(Bytes.toBytes("f1"), Bytes.toBytes("username")))


          val hbaseCustomer = new HBaseCustomer(name, birthdate, mail, sex, username)

          val combinedData = new CombinedData(customer, hbaseCustomer)
          println(combinedData)

          combinedData
        })

      })


      result.printSchema()

      val query = result.writeStream
        .outputMode(OutputMode.Append())
        .format("parquet")
        .option("path", "hdfs://quickstart.cloudera:8020/user/nickrafferty78/reviews")
        .option("checkpointLocation", "hdfs://quickstart.cloudera:8020/user/nickrafferty78/reviews_checkpoint")
        .trigger(Trigger.ProcessingTime("5 seconds"))
        .start()



      query.awaitTermination()
    } catch {
      case e: Exception => logger.error(s"$jobName error in main", e)
    }
  }

  def compute(df: DataFrame): DataFrame = {

    df.select(from_json(df("value"), schema) as "jsonData")

  }
}
//CREATE TABLE IF NOT EXISTS customerReviews ( customerReview: struct, marketplace: string,
//customer_id: string, review_id: string,product_id: string, product_parent: int,
// product_title: string,product_category: string,star_rating: int,
// helpful_votes: int,total_votes: int,vine: string,verified_purchase: string,review_headline: string,
// review_body: string,review_date: timestamp, HBaseCustomer: struct,
//name: string, birthdate: string,mail: string,sex: string,username: string)
//COMMENT ‘Customer details’
//STORED AS PARQUET;
