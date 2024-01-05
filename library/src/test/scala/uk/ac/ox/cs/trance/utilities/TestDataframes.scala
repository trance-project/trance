package uk.ac.ox.cs.trance.utilities

import framework.common.CaseClassRecord
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

  lazy val simplerMultiNestedDataframe: DataFrame = {

    val data = Seq(("Java", Seq(Seq(true, 5L))), ("Python", Seq(Seq(true, 10L))), ("Scala", Seq(Seq(false, 8L))))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map {
      case (language, Seq(Seq(active, level))) =>
        Row(language, Row(Row(active, level)))
    }

    val schema: StructType = StructType(Seq(
      StructField("language", StringType, nullable = true),
      StructField("stats", StructType(Seq(
        StructField("users", StructType(Seq(
          StructField("active", BooleanType, nullable = true),
          StructField("level", LongType, nullable = true)
        ))),
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

  lazy val LineItem: DataFrame = {
    val lineitemSchema = StructType(
      Seq(
        StructField("l_orderkey", IntegerType, nullable = false),
        StructField("l_partkey", IntegerType, nullable = false),
        StructField("l_suppkey", IntegerType, nullable = false),
        StructField("l_linenumber", IntegerType, nullable = false),
        StructField("l_quantity", DoubleType, nullable = false),
        StructField("l_extendedprice", DoubleType, nullable = false),
        StructField("l_discount", DoubleType, nullable = false),
        StructField("l_tax", DoubleType, nullable = false),
        StructField("l_returnflag", StringType, nullable = false),
        StructField("l_linestatus", StringType, nullable = false),
        StructField("l_shipdate", StringType, nullable = false),
        StructField("l_commitdate", StringType, nullable = false),
        StructField("l_receiptdate", StringType, nullable = false),
        StructField("l_shipinstruct", StringType, nullable = false),
        StructField("l_shipmode", StringType, nullable = false),
        StructField("l_comment", StringType, nullable = false),
        StructField("uniqueId", LongType, nullable = false)
      )
    )

    val data = Seq(
      Row(1, 101, 201, 1, 10.5, 100.0, 0.1, 0.05, "R", "Shipped", "2022-01-01", "2022-01-02", "2022-01-03", "Air", "Express", "Comment1", 1001L),
      Row(2, 102, 202, 2, 20.5, 200.0, 0.2, 0.1, "N", "Pending", "2022-02-01", "2022-02-02", "2022-02-03", "Ground", "Standard", "Comment2", 1002L),
      Row(3, 103, 203, 3, 30.5, 300.0, 0.3, 0.15, "Y", "Shipped", "2022-03-01", "2022-03-02", "2022-03-03", "Sea", "Express", "Comment3", 1003L))

    spark.createDataFrame(spark.sparkContext.parallelize(data), lineitemSchema)
  }

  lazy val Order: DataFrame = {
    val orderSchema = StructType(
      Seq(
        StructField("o_orderkey", IntegerType, nullable = false),
        StructField("o_custkey", IntegerType, nullable = false),
        StructField("o_orderstatus", StringType, nullable = false),
        StructField("o_totalprice", DoubleType, nullable = false),
        StructField("o_orderdate", StringType, nullable = false),
        StructField("o_orderpriority", StringType, nullable = false),
        StructField("o_clerk", StringType, nullable = false),
        StructField("o_shippriority", IntegerType, nullable = false),
        StructField("o_comment", StringType, nullable = false)
      )
    )

    val data = Seq(Row(1, 101, "Shipped", 1000.0, "2022-01-01", "High", "Clerk1", 1, "Comment1"),
      Row(2, 102, "Pending", 2000.0, "2022-02-01", "Medium", "Clerk2", 2, "Comment2"),
      Row(1, 103, "Shipped", 1500.0, "2022-03-01", "High", "Clerk3", 3, "Comment3"))

    spark.createDataFrame(spark.sparkContext.parallelize(data) , orderSchema)
  }

  lazy val Customer: DataFrame = {
    val customerSchema = StructType(Seq(
      StructField("c_custkey", IntegerType),
      StructField("c_name", StringType),
      StructField("c_address", StringType),
      StructField("c_nationkey", IntegerType),
      StructField("c_phone", StringType),
      StructField("c_acctbal", DoubleType),
      StructField("c_mktsegment", StringType),
      StructField("c_comment", StringType)
    ))

    val data = Seq(
      Row(101, "John Doe", "123 Main St", 1, "555-1234", 1000.0, "Retail", "Good customer"),
      Row(102, "Alice Smith", "456 Oak St", 2, "555-5678", 1500.0, "Wholesale", "Valued client"),
      Row(109, "Bob Johnson", "789 Pine St", 3, "555-9876", 800.0, "Retail", "Regular shopper")
    )
    spark.createDataFrame(spark.sparkContext.parallelize(data), customerSchema)
  }


}
