package uk.ac.ox.cs.trance.utilities

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import java.sql.Date

object TestDataframes {

  val spark: SparkSession = SparkSession.builder
    .appName("TestDataframes")
    .master("local[1]")
    .getOrCreate()

  lazy val simpleIntDataframe: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Java", 90), ("Rust", 100), ("Go", 10))
    import spark.implicits._
    data.toDF("language", "users")
  }

  lazy val simpleIntDataframe2: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
    import spark.implicits._
    data.toDF("lng", "usr")
  }

  lazy val simpleIntDataframe3: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 100), ("Ruby", 10), ("Rust", 5), ("Go", 3))
    import spark.implicits._
    data.toDF("lName", "userNo")
  }

  lazy val simpleIntDataframe4: DataFrame = {
    val data: Seq[(String, Int)] = Seq(("Go", 20), ("Java", 1), ("Rust", 3), ("Go", 4))
    import spark.implicits._
    data.toDF("language", "users")
  }

  lazy val simpleIntDataframe5: DataFrame = {
    val data: Seq[(String, Int, Boolean)] = Seq(("Go", 20, true), ("Java", 1, true), ("Rust", 3, false), ("Go", 4, false))
    import spark.implicits._
    data.toDF("language", "users", "inUse")
  }

  lazy val simpleStringDataframe: DataFrame = {
    val data = Seq(("C#", "100", true), ("C++", "250", false), ("C#", "100", true))
    import spark.implicits._
    data.toDF("language", "users", "inUse")
  }

  lazy val simpleAllTypesDataframe: DataFrame = {
    val data = Seq(("C#", 100, true, 7.5, 75L), ("C++", 250, false, 2.9, 25L), ("Java", 300, true, 3.5, 35L))
    import spark.implicits._
    data.toDF("language", "users", "inUse", "weight", "percentage")
  }

  lazy val pureIntDataframe: DataFrame = {
    val data: Seq[(Int, Int)] = Seq((1, 20), (2, 90), (3, 100), (4, 10))
    import spark.implicits._
    data.toDF("id", "users")
  }

  lazy val pureStringDataframe: DataFrame = {
    val data: Seq[(String, String)] = Seq(("1", "20"), ("2", "90"), ("3", "100"), ("4", "10"))
    import spark.implicits._
    data.toDF("id", "users")
  }


  lazy val nestedDataframe: DataFrame = {

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

  lazy val nestedDataframe2: DataFrame = {

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
  lazy val nestedDataframe3: DataFrame = {

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
  lazy val nestedDataframe4: DataFrame = {

    val data = Seq(("Java", Seq("10", 0, 7.5)), ("Python", Seq("10000", 2, 8.5)), ("Scala", Seq("3000", 2, 9.0)))

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
  lazy val nestedDataframe5: DataFrame = {

    val data = Seq(("Java", Seq("10", 0, 7.5)), ("Python", Seq("10000", 2, 8.5)), ("Scala", Seq("30000", 2, 9.0)))

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

  lazy val multiNestedDataframe: DataFrame = {

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

  lazy val arrayTypeDataframe: DataFrame = {
    val arrayStructureData = Seq(
      Row("James, Smith", List("Java", "Scala", "C++"), "OH"),
      Row("Michael, Rose", List("Spark", "Java", "C++"), "NJ"),
      Row("Robert, Williams", List("CSharp", "VB"), "NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("language", ArrayType(StringType))
      .add("currentState", StringType)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

    df
  }

  lazy val arrayTypeDataframe2: DataFrame = {
    val arrayStructureData = Seq(
      Row("James, Smith", List("Java", "Scala", "C++"), "OH"),
      Row("Michael, Rose", List("C#", "Java", "C++"), "NJ"),
      Row("Robert, Williams", List("Go", "VB"), "NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("language", ArrayType(StringType))
      .add("currentState", StringType)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

    df
  }

  lazy val COP: DataFrame = {
    val inputSchema: StructType = StructType(Seq(
      StructField("cname", StringType),
      StructField("corders", StructType(Seq(
        StructField("odate", DateType),
        StructField("oparts", StructType(Seq(
          StructField("pid", IntegerType),
          StructField("qty", DoubleType)
        ))))))))

    val exampleRow = Seq(
      Row("test1", Row(Date.valueOf("2023-01-01"), Row(1, 1.5),
        Row("test2", Row(Date.valueOf("2023-01-02"), Row(2, 2.5)
      )))))

    val outputSchema: StructType = StructType(Seq(
      StructField("cname", StringType),
      StructField("corders", StructType(Seq(
        StructField("odate", DateType),
        StructField("oparts", StructType(Seq(
          StructField("pname", StringType),
          StructField("total", DoubleType)
        ))))))))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(exampleRow), inputSchema)

    df
  }

  lazy val PART: DataFrame = {
    val inputSchema: StructType = StructType(Seq(
      StructField("pid", IntegerType),
      StructField("pname", StringType),
      StructField("price", DoubleType)))

    val exampleRow = Seq(
      Row(1, "testPName", 2.5),
      Row(3, "testPName2", 3.5),
    )

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(exampleRow), inputSchema)

    df
  }

}
