package uk.ac.ox.cs.trance.utilities

import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSparkSession: SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("TranceTest")
//      .config("spark.serializer.extraDebugInfo", "true")
      .getOrCreate()

//    spark.sparkContext.setLogLevel("DEBUG")
    spark
  }
}
