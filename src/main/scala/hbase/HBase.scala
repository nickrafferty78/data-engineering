package hbase

import org.apache.hadoop.hbase.{Cell, HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get, Put, ResultScanner, Scan}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.logging.log4j.{LogManager, Logger}

object HBase {

  lazy val logger: Logger = LogManager.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    System.out.println("Starting...");
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "35.184.255.239")
      connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("nickrafferty78:users"))
      val get = new Get(Bytes.toBytes("36442766"))
      val result = table.get(get)
      System.out.println(result)
      logger.info(result)


      val scan = new Scan().withStartRow(Bytes.toBytes("10000001")).withStopRow(Bytes.toBytes("10006001"))
      import scala.collection.JavaConverters._
      val count = table.getScanner(scan).asScala.seq
      System.out.println(count)
      logger.info(count)



    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }

}
