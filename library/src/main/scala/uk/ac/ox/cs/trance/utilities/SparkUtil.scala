package uk.ac.ox.cs.trance.utilities

import org.apache.spark.sql.SparkSession

object SparkUtil {
  def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[1]")
      .appName("RDDTest")
      .getOrCreate()
  }
}
