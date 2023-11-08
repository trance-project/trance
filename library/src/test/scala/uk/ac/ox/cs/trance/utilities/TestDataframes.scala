package uk.ac.ox.cs.trance.utilities

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object TestDataframes {

    val spark: SparkSession = SparkSession.builder
      .appName("TestDataframes")
      .master("local[1]")
      .getOrCreate()

  val simpleIntDataframe: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Java", 90), ("Rust", 100), ("Go", 10))
    import spark.implicits._
    data.toDF("language", "users")
  }

  val simpleIntDataframe2: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
    import spark.implicits._
    data.toDF("lng", "usr")
  }

  val simpleIntDataframe3: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 100), ("Ruby", 10), ("Rust", 5), ("Go", 3))
    import spark.implicits._
    data.toDF("lName", "userNo")
  }

  val simpleIntDataframe4: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Java", 1), ("Rust", 3), ("Go", 4))
    import spark.implicits._
    data.toDF("language", "users")
  }


  val simpleStringDataframe: DataFrame = {
    val data = Seq(("C#", "100", true), ("C++", "250", false), ("C#", "100", true))
    import spark.implicits._
    data.toDF("language", "users", "inUse")
  }

  val nestedDataframe: DataFrame = {

    val data = Seq(("Java", Seq("20000", 0, 7.5)), ("Python", Seq("100000", 1, 8.5)), ("Scala", Seq("3000", 2, 9.0)))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, Row.fromSeq(s)) }

    val schema: StructType = StructType(Seq(
      StructField("language", StringType, nullable = true),
      StructField("info", StructType(Seq(
        StructField("users", StringType, nullable = true),
        StructField("difficulty", IntegerType, nullable = true),
        StructField("average_review", DoubleType, nullable = true)
      )))
    ))

    spark.createDataFrame(rdd, schema)

  }

  val nestedDataframe2: DataFrame = {

    val data = Seq(("Java", Seq("20000", 0, 7.5)), ("Python", Seq("100000", 1, 8.5)), ("Scala", Seq("3000", 2, 9.0)))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, Row.fromSeq(s)) }

    val schema: StructType = StructType(Seq(
      StructField("language", StringType, nullable = true),
      StructField("stats", StructType(Seq(
        StructField("users", StringType, nullable = true),
        StructField("difficulty", IntegerType, nullable = true),
        StructField("average_review", DoubleType, nullable = true)
      )))
    ))

    spark.createDataFrame(rdd, schema)

  }

  val multiNestedDataframe: DataFrame = {

    val data = Seq(("Java", Seq(Seq(true, 5L), 0, 7.5)), ("Python", Seq(Seq(true, 10L), 1, 8.5)), ("Scala", Seq(Seq(false, 8L), 2, 9.0)))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map {
      case (language, Seq(Seq(active, level), difficulty, average_review)) =>
        Row(language, Row(Row(active, level), difficulty, average_review))
    }

    val schema: StructType = StructType(Seq(
      StructField("language", StringType, nullable = true),
      StructField("stats", StructType(Seq(
        StructField("users", StructType(Seq(
          StructField("active", BooleanType, nullable = true),
          StructField("level", LongType, nullable = true)
        ))),
        StructField("difficulty", IntegerType, nullable = true),
        StructField("average_review", DoubleType, nullable = true)
      )))
    ))

    spark.createDataFrame(rdd, schema)

  }
}
