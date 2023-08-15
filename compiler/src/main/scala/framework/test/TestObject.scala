package framework.test
//import framework.library.WrapDataset.addWrap

import framework.library.Dataset.wrap
import framework.library.utilities.SparkUtil.getSparkSession
import framework.library._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object TestObject {

  val spark: SparkSession = getSparkSession

  def main(args: Array[String]): Unit = {
    val ds: DataFrame = simpleStringDataframe()
    val di: DataFrame = simpleStringDataframe2()
    val dt: DataFrame = simpleStringDataframe3()

    val wrappedD = ds.wrap()
    val wrappedD2 = di.wrap()

    val e1 = wrappedD.union(wrappedD2).dropDuplicates

    // TODO - Comprehensions occur in final cExpr with the following operation
    //val e4 = wrappedD.union(wrappedD2.flatMap(y => wrappedD.flatMap(_ => Sng(y))))

    // TODO Select can't be used with other operations (DeDup, Union etc.) because of Project != BagExpr
//    val e4 = wrappedD.dropDuplicates.select("language")

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

  private def simpleStringDataframe2(): DataFrame = {

    val data = Seq(("Java", "20000"), ("Ruby", "900"), ("Rust", "100"))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", StringType, nullable = true)
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

    val data = Seq(("Go", 80000), ("Ruby", 900), ("Rust", 100))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", IntegerType, nullable = true)
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
}
