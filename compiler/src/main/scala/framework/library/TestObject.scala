package framework.library

import CustomFunctions._
import framework.library.utilities.SparkUtil.getSparkSession
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types._

object TestObject {

  val spark: SparkSession = getSparkSession

  def main(args: Array[String]): Unit = {
    val ds: Dataset[Row] = simpleStringObject()
    val di: Dataset[Row] = simpleIntObject()
    println("Before Pipeline: ")
    ds.printSchema()
    ds.show()

    val e1: WrappedDataset = ds.wrap()
    val e2: WrappedDataset = di.wrap()

    val e3 = e1.flatMap(x => e2)(e1, e2)

    val resultDF = e3.leaveNRC()

    val ds3: DataFrame = e1.leaveNRC()

    println("After Pipeline: ")
    ds3.show()
    ds3.printSchema()
  }

  private def simpleStringObject(): DataFrame = {

    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", StringType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def simpleIntObject(): DataFrame = {

    val data = Seq(("Java", 20000), ("Python", 100000), ("Scala", 3000))

    val rdd: RDD[Row] = spark.sparkContext.parallelize(data).map { case (l, s) => Row(l, s) }

    val schema: StructType = StructType(Array(
      StructField("language", StringType, nullable = true),
      StructField("users", IntegerType, nullable = true)
    ))

    spark.createDataFrame(rdd, schema)

  }

  private def nestedObject(): DataFrame = {

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

  private def multiNestedObjected(): DataFrame = {

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

  private def arrayTypeObject(): DataFrame = {
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
