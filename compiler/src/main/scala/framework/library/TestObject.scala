package framework.library

import CustomFunctions._
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.SQLContext._

object TestObject {
  def main(args: Array[String]): Unit = {
    val ds = simpleStringObject()

    println("Before Pipeline: ")
    ds.printSchema()
    ds.show()

    val scalaNRC = ds.enterNRC()
    scalaNRC.flatMap()
    val ds2 = scalaNRC.leaveNRC()

    println("After Pipeline: ")
    ds2.show()
    ds.printSchema()
  }

  def simpleStringObject(): DataFrame = {
    val spark = utilities.SparkUtil.getSparkSession()

    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)

    val schema = StructType(Array(
      StructField("language", StringType, true),
      StructField("users", StringType, true)
    ))

    val rowRDD = rdd.map(attributes => Row(attributes._1, attributes._2))
    val ds = spark.createDataFrame(rowRDD, schema)
    ds
  }

  // second dataset with nested data
  def nestedObject() = {
    val spark = utilities.SparkUtil.getSparkSession()

    val data = Seq(("Java", Seq("20000", "E")), ("Python", Seq("100000", "M")), ("Scala", Seq("3000", "H")))
    val rdd = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, Row(s(0), s(1)))}

    val schema = StructType(Seq(
      StructField("language", StringType, true),
      StructField("stats", StructType(Seq(
        StructField("users", StringType, true),
        StructField("difficulty", StringType, true)
      )))
    ))

    val rowRDD = rdd.map(attributes => Row(attributes.get(0), attributes.get(1)))
    val ds = spark.createDataFrame(rowRDD, schema)

    ds.printSchema()
    ds.show(false)

    ds

  }
}
