package net.fawad.learning.testingsparkjdbc

import org.apache.log4j.LogManager
import org.apache.spark.sql.SparkSession

object Main extends App {
  val logger = LogManager.getLogger(this.getClass)
  val ss = SparkSession.builder().config("spark.master", "local[*]").getOrCreate()
  try {
    val sqlContext = ss.sqlContext
    val data = sqlContext.read.format("jdbc").
      options(Map(
        "url" -> "jdbc:postgresql://localhost/fawad?user=fawad&password=foobar",
        "dbtable" -> "(SELECT * FROM generate_series(1, 1000000)) AS thedata")).load()
    logger.info("Reading data...")
    data.foreach(x=> logger.info(s"Received record ${x}"))
  } finally {
    ss.stop()
  }
}