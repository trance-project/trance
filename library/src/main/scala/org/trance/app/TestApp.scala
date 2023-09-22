package org.trance.app

import org.trance.nrclibrary.Wrapper.DataFrameImplicit
import org.trance.nrclibrary.utilities.SparkUtil.getSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.trance.nrclibrary.Sng

object TestApp {

  val spark: SparkSession = getSparkSession

  def main(args: Array[String]): Unit = {
    val ds: DataFrame = simpleStringDataframe()
    val di: DataFrame = simpleStringDataframe2()
    val dt: DataFrame = simpleStringDataframe3()
    val intDf: DataFrame = simpleIntDataframe()
    val intDf2: DataFrame = simpleIntDataframe2()
    val d5: DataFrame = simpleStringDataframeSpecial()

//    val d3 = d5.map(x => {
//      (x.getString(0), x.getString(1))
//    })
//
//    d3.show()




//    val sparkRes = intDf.join(intDf2, intDf("users") <= intDf2("usersCount") || intDf("users") > intDf2("usersCount"))
    val sparkRes = intDf.select(intDf("users") * intDf("users"), intDf("language"))
    sparkRes.show()

    val wrappedD = ds.wrap()
    val wrappedD2 = di.wrap()
    val wrappedD3 = intDf.wrap()
    val wrappedD4 = dt.wrap()
    val wrappedD5 = d5.wrap()
    val wrappedIntDf = intDf.wrap()
    val wrappedIntDf2 = intDf2.wrap()

//    val e1 = wrappedIntDf.flatMap(x => Sng(x))

//    val e1 = wrappedIntDf.join(wrappedIntDf2, wrappedIntDf("users") === wrappedIntDf2("usersCount"), "inner")
//    val e1 = wrappedIntDf.union(wrappedIntDf.flatMap(x => Sng(x)))
    val e1 = wrappedIntDf.select(wrappedIntDf("users") * wrappedIntDf("users"), wrappedIntDf("language"))


//    val e1 = wrappedIntDf.join(wrappedIntDf2, wrappedIntDf("users") <= wrappedIntDf2("usersCount") || wrappedIntDf("users") > wrappedIntDf2("usersCount"), "inner")

    val d = e1.leaveNRC()
    d.show()
  }

  private def simpleStringDataframe(): DataFrame = {

    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", StringType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }


  private def simpleStringDataframeSpecial(): DataFrame = {

    val data = Seq(("Java", "20000"), ("Ruby", "900"), ("Rust", "200"))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("programmingLanguage", StringType, nullable = true),
      StructField("userCount", StringType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def simpleStringDataframe2(): DataFrame = {

    val data = Seq(("Java", "20000"), ("Ruby", "900"), ("Rust", "100"))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("programmingLanguage", StringType, nullable = true),
      StructField("userCount", StringType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def simpleStringDataframe3(): DataFrame = {

    val data = Seq(("C#", "100", true), ("C++", "250", false), ("C#", "100", true))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s, b) => Row(l, s, b) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", StringType, nullable = true),
      StructField("included", BooleanType, nullable = false)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def simpleIntDataframe(): DataFrame = {

    val data = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", IntegerType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def simpleIntDataframe2(): DataFrame = {

    val data = Seq(("Go", 1000000), ("Ruby", 10), ("Rust", 100), ("Go", 80))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("programmingLanguage", StringType, nullable = true),
      StructField("usersCount", IntegerType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def nestedDataframe(): DataFrame = {

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

  private def multiNestedDataframe(): DataFrame = {

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

  private def arrayTypeDataframe(): DataFrame = {
    val arrayStructureData = Seq(
      Row("James,,Smith",List("Java","Scala","C++"),"OH"),
      Row("Michael,Rose,",List("Spark","Java","C++"),"NJ"),
      Row("Robert,,Williams",List("CSharp","VB"),"NV")
    )
    val arrayStructureSchema = new StructType()
      .add("name",StringType)
      .add("languagesAtSchool", ArrayType(StringType))
      .add("currentState", StringType)
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(arrayStructureData),arrayStructureSchema)

    df

  }

//  private def jDataframe(): DataFrame = {
//    val schema: StructType = StructType(Seq(
//      StructField("cname", StringType, nullable = true),
//      StructField("corders", StructType(Seq(
//        StructField("odate", StringType, nullable = true),
//        StructField("oparts", StructType(Seq(
//          StructField("pid", IntegerType),
//          StructField("qty", IntegerType)
//        ))),
//      )))
//    ))
//
//    val COP = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row())), schema)
//
//    val PART = spark.createDataFrame(spark.sparkContext.parallelize(Seq(Row())), schema)
//
//    COP.flatMap{ cop =>
//      COP("cname") -> cop.getString(0)
//      COP("corder") -> cop.get(1).asInstanceOf[Seq[Row]].flatMap {
//        co => COP("odate") -> co.getString(0)
//          COP("oparts") -> co.getString(1).asInstanceOf[Seq[Row]].flatMap { op =>
//              PART.flatMap(p =>
//                if (op.getInt(0) == p.getInt(0)) { Seq(
//                PART("pname") -> p.getString(1),
//                PART("total") -> (op.getInt(1) * p.getInt(2))
//                )
//                }
//                else null
//              ).groupBy("pname").sum("total")
//          }
//      }
//    }
//  }
}
