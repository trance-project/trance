package uk.ac.ox.cs.trance.utilities

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

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

  // TODO array of primitive type doesnt convert to NRC Currently
  lazy val arrayTypeDataframe2: DataFrame = {
    val arrayStructureData = Seq(
      Row("James, Smith", List(Row("Java"), Row("Scala")), "OH"),
      Row("Michael, Rose", List(Row("C#"), Row("C")), "NJ"),
    )
    val arrayStructureSchema = new StructType()
      .add("name", StringType)
      .add("languageClass", ArrayType(StructType(Seq(StructField("language", StringType)))))
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData), arrayStructureSchema)

    df
  }

  lazy val Test2FullWithMultipleArrayTypes = {
    val schema = StructType(Array(
      StructField("c_acctbal", DoubleType, false),
      StructField("c_name", StringType, true),
      StructField("Customer_index", LongType, false),
      StructField("c_nationkey", IntegerType, false),
      StructField("c_custkey", IntegerType, false),
      StructField("c_comment", StringType, true),
      StructField("c_address", StringType, true),
      StructField("c_orders", ArrayType(StructType(Array(
        StructField("o_shippriority", IntegerType, false),
        StructField("o_orderdate", StringType, true),
        StructField("o_custkey", IntegerType, false),
        StructField("o_orderpriority", StringType, true),
        StructField("o_parts", ArrayType(StructType(Array(
          StructField("l_returnflag", StringType, true),
          StructField("l_comment", StringType, true),
          StructField("l_linestatus", StringType, true),
          StructField("l_shipmode", StringType, true),
          StructField("l_shipinstruct", StringType, true),
          StructField("l_quantity", DoubleType, false),
          StructField("l_receiptdate", StringType, true),
          StructField("l_linenumber", IntegerType, false),
          StructField("l_tax", DoubleType, false),
          StructField("l_shipdate", StringType, true),
          StructField("l_extendedprice", DoubleType, false),
          StructField("l_partkey", IntegerType, false),
          StructField("l_discount", DoubleType, false),
          StructField("l_commitdate", StringType, true),
          StructField("l_suppkey", IntegerType, false),
          StructField("l_orderkey", IntegerType, false)
        )), true)),
        StructField("o_clerk", StringType, true),
        StructField("o_orderstatus", StringType, true),
        StructField("o_totalprice", DoubleType, false),
        StructField("o_orderkey", IntegerType, false),
        StructField("o_comment", StringType, true)
      )), true)),
      StructField("c_mktsegment", StringType, true),
      StructField("c_phone", StringType, true)
    ))

    val data = Seq(
      Row(800.0, "Bob Johnson", 8589934593L, 3, 109, "Regular shopper", "789 Pine St", Array(), "Retail", "555-9876"),
      Row(1000.0, "John Doe", 0L, 1, 101, "Good customer", "123 Main St", Array(
        Row(1, "2022-01-01", 101, "High", Array(
          Row("R", "Comment1", "Shipped", "Express", "Air", 10.5, "2022-01-03", 1, 0.05, "2022-01-01", 100.0, 101, 0.1, "2022-01-02", 201, 1),
          Row("S", "Comment1.1", "Shipped", "Sea", "Surface", 15.0, "2022-01-04", 2, 0.07, "2022-01-02", 150.0, 102, 0.15, "2022-01-03", 202, 2)
        ), "Clerk1", "Shipped", 1000.0, 1, "Comment1"),
        Row(2, "2022-02-01", 102, "Medium", Array(
          Row("N", "Comment2", "Pending", "Standard", "Ground", 20.5, "2022-02-03", 2, 0.1, "2022-02-01", 200.0, 2, 0.2, "2022-02-02", 202, 2),
          Row("O", "Comment2.1", "Pending", "Rail", "Rail", 25.0, "2022-02-04", 3, 0.12, "2022-02-02", 250.0, 3, 0.25, "2022-02-03", 203, 3)
        ), "Clerk2", "Pending", 2000.0, 2, "Comment2")
      ), "Wholesale", "555-5678")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

  lazy val Test2FullWithMultipleArrayTypesLessRows = {
    val schema = StructType(Array(
      StructField("c_acctbal", DoubleType, false),
      StructField("c_name", StringType, true),
      StructField("Customer_index", LongType, false),
      StructField("c_nationkey", IntegerType, false),
      StructField("c_custkey", IntegerType, false),
      StructField("c_comment", StringType, true),
      StructField("c_address", StringType, true),
      StructField("c_orders", ArrayType(StructType(Array(
        StructField("o_shippriority", IntegerType, false),
        StructField("o_orderdate", StringType, true),
        StructField("o_custkey", IntegerType, false),
        StructField("o_orderpriority", StringType, true),
        StructField("o_parts", ArrayType(StructType(Array(
          StructField("l_returnflag", StringType, true),
          StructField("l_comment", StringType, true),
          StructField("l_linestatus", StringType, true),
          StructField("l_shipmode", StringType, true),
          StructField("l_shipinstruct", StringType, true),
          StructField("l_quantity", DoubleType, false),
          StructField("l_receiptdate", StringType, true),
          StructField("l_linenumber", IntegerType, false),
          StructField("l_tax", DoubleType, false),
          StructField("l_shipdate", StringType, true),
          StructField("l_extendedprice", DoubleType, false),
          StructField("l_partkey", IntegerType, false),
          StructField("l_discount", DoubleType, false),
          StructField("l_commitdate", StringType, true),
          StructField("l_suppkey", IntegerType, false),
          StructField("l_orderkey", IntegerType, false)
        )), true)),
        StructField("o_clerk", StringType, true),
        StructField("o_orderstatus", StringType, true),
        StructField("o_totalprice", DoubleType, false),
        StructField("o_orderkey", IntegerType, false),
        StructField("o_comment", StringType, true)
      )), true)),
      StructField("c_mktsegment", StringType, true),
      StructField("c_phone", StringType, true)
    ))

    val data = Seq(
      Row(800.0, "Bob Johnson", 8589934593L, 3, 109, "Regular shopper", "789 Pine St", Array(), "Retail", "555-9876"),
      Row(1000.0, "John Doe", 0L, 1, 101, "Good customer", "123 Main St", Array(
        Row(1, "2022-01-01", 101, "High", Array(
          Row("R", "Comment1", "Shipped", "Express", "Air", 10.5, "2022-01-03", 1, 0.05, "2022-01-01", 100.0, 101, 0.1, "2022-01-02", 201, 1),
          Row("S", "Comment1.1", "Shipped", "Sea", "Surface", 15.0, "2022-01-04", 2, 0.07, "2022-01-02", 150.0, 102, 0.15, "2022-01-03", 202, 2)
        ), "Clerk1", "Shipped", 1000.0, 1, "Comment1"),
      ), "Wholesale", "555-5678")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
  }

}
