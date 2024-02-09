package uk.ac.ox.cs.trance.utilities

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

import java.sql.Date

object TPCHDataframes {

  val spark: SparkSession = SparkSession.builder
    .appName("TPCHDataframes")
    .master("local[2]")
    .getOrCreate()

  lazy val COP: DataFrame = {
    val inputSchema: StructType = StructType(Seq(
      StructField("cname", StringType),
      StructField("corders", StructType(Seq(
        StructField("odate", StringType),
      )))))

    val exampleRow = Seq(
      Row("test1", Row("2023-01-01"),
        Row("test2", Row("2023-01-02")
        )))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(exampleRow), inputSchema)

    df
  }

  lazy val Part: DataFrame = {
    val partSchema = StructType(Seq(
      StructField("p_partkey", IntegerType, nullable = false),
      StructField("p_name", StringType, nullable = false),
      StructField("p_mfgr", StringType, nullable = false),
      StructField("p_brand", StringType, nullable = false),
      StructField("p_type", StringType, nullable = false),
      StructField("p_size", IntegerType, nullable = false),
      StructField("p_container", StringType, nullable = false),
      StructField("p_retailprice", DoubleType, nullable = false),
      StructField("p_comment", StringType, nullable = false)
    ))

    val data = Seq(
      Row(1, "Part1", "Mfgr1", "Brand1", "Type1", 1, "Container1", 100.0, "Comment1"),
      Row(2, "Part2", "Mfgr2", "Brand2", "Type2", 2, "Container2", 200.0, "Comment2"),
      Row(3, "Part3", "Mfgr3", "Brand3", "Type3", 3, "Container3", 300.0, "Comment3")
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), partSchema)
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
      Row(2, 2, 202, 2, 20.5, 200.0, 0.2, 0.1, "N", "Pending", "2022-02-01", "2022-02-02", "2022-02-03", "Ground", "Standard", "Comment2", 1002L),
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

    spark.createDataFrame(spark.sparkContext.parallelize(data), orderSchema)
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

  lazy val Nation: DataFrame = {
    val nationSchema = StructType(Seq(
      StructField("n_nationkey", IntegerType, nullable = false),
      StructField("n_name", StringType, nullable = false),
      StructField("n_regionkey", IntegerType, nullable = false),
      StructField("n_comment", StringType, nullable = false),
      StructField("uniqueId", LongType, nullable = false)
    ))

    val data = Seq(
      Row(1, "Nation1", 1, "Comment1", 1L),
      Row(2, "Nation2", 2, "Comment2", 2L),
      Row(3, "Nation3", 3, "Comment3", 3L)
    )

    spark.createDataFrame(spark.sparkContext.parallelize(data), nationSchema)

  }
}
