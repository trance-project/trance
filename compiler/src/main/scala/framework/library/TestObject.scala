package framework.library

import CustomFunctions._
import framework.library.WrappedDataset._
import framework.library.utilities.SparkUtil.getSparkSession
import framework.nrc.{BaseExpr, NRC}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object TestObject {

  val spark: SparkSession = getSparkSession

  def main(args: Array[String]): Unit = {
    val ds: Dataset[Row] = simpleStringDataframe()
    val di: Dataset[Row] = simpleIntDataframe()

    println("Before Pipeline: ")
    ds.printSchema()
    ds.show()

    val e1: WrappedDataset = ds.wrap()
    val e2: WrappedDataset = di.wrap()

    val e3 = e1.flatMap(_ => e2)

    val e4 = e1.flatMap(x => Singleton(x))

    val ds3 = e3.leaveNRC()
    val ds4 = e4.leaveNRC()


    println("After Pipeline: ")
    ds3.show()
    ds3.printSchema()
    ds4.show()
    ds4.printSchema()
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
