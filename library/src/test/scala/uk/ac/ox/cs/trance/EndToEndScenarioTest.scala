package uk.ac.ox.cs.trance


import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.DataFrame
import uk.ac.ox.cs.trance.app.TestApp.spark
import Wrapper.DataFrameImplicit
import utilities.{JoinContext, Symbol}
import org.apache.spark.sql.functions.col
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.TestDataframes._

class EndToEndScenarioTest extends AnyFunSpec with BeforeAndAfterEach {


  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  //  def assertDataFramesAreEquivalent(df1: DataFrame, df2: DataFrame): Unit = {
  //    implicit val anyOrdering: Ordering[Any] = Ordering.fromLessThan {
  //      case (a, b) => a.toString < b.toString
  //    }
  //
  //    val set1 = df1.collect().map(_.toSeq.sorted).toSet
  //    val set2 = df2.collect().map(_.toSeq.sorted).toSet
  //
  //    set1 shouldEqual set2
  //  }

  def assertDataFramesAreEquivalent(df1: DataFrame, df2: DataFrame): Unit = {
    implicit val anyOrdering: Ordering[Any] = Ordering.fromLessThan {
      case (a, b) => a.toString < b.toString
    }

    val count1 = df1.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)
    val count2 = df2.collect().map(_.toSeq.sorted).groupBy(identity).mapValues(_.length)

    count1 shouldEqual count2
  }

  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatMap") {
    it("FlatMap Singleton") {
      val wrappedDataframe = simpleIntDataframe.wrap()

      val res = wrappedDataframe.flatMap(x => Sng(x))

      assertDataFrameEquals(simpleIntDataframe, res.leaveNRC())

    }
  }

  describe("Merge") {
    it("Successful Merge - 2 Flat Datasets") {
      val wrappedDataframe = simpleIntDataframe.wrap()
      val wrappedDataframe2 = simpleIntDataframe.wrap()

      val res = wrappedDataframe.union(wrappedDataframe2).leaveNRC()
      val expected = simpleIntDataframe.union(simpleIntDataframe)

      assertDataFrameEquals(res, expected)
    }

    it("Unsuccessful Merge - Incompatible Dataframe Structures") {
      val wrappedDataframe = simpleIntDataframe.wrap()
      val wrappedDataframe2 = simpleStringDataframe.wrap()

      val caughtException = intercept[Throwable] {
        wrappedDataframe.union(wrappedDataframe2).leaveNRC()
      }

      assert(caughtException.isInstanceOf[AssertionError])
    }

  }

  describe("Join") {

    it("Successful Join - 2 Flat Datasets with Distinct Column Names") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe3
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val expected = df.join(df2, df("language") === df2("lName"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("lName")).leaveNRC()

      assertDataFrameEquals(res, expected)
    }

    it("Successful Join - 2 Flat Datasets with Distinct Join Column & other Non Distinct Columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("lName", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("lName"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("lName")).leaveNRC()

      assertDataFrameEquals(res, expected)

    }
    it("Successful Join - 2 Flat Datasets Non Distinct Column on Join Condition") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "stats")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)

    }

    it("Successful Self Join - Column Equality") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.join(df, df("language") === df("language"))
      val res = wrappedDf.join(wrappedDf, wrappedDf("language") === wrappedDf("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Self Join - Column Inequality") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.join(df, df("language") =!= df("language"))
      val res = wrappedDf.join(wrappedDf, wrappedDf("language") =!= wrappedDf("language")).leaveNRC()

      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - Integer Equality Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.join(df, df("users") === 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") === 10).leaveNRC()

      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - Integer Inequality Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.join(df, df("users") =!= 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") =!= 10).leaveNRC()

      assertDataFrameEquals(res, expected)
    }

    // These next few don't work the same in spark, you have to specify which side you are interested in manually,
    // trance automatically chooses left side
    it("Successful Self Join - Integer LessThanEqual Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.alias("left").join(df.alias("right"), col("left.users") <= 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") <= 10).leaveNRC()
      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - Integer GreaterThanEqual Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.alias("left").join(df.alias("right"), col("left.users") >= 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") >= 10).leaveNRC()
      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - Integer LessThan Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.alias("left").join(df.alias("right"), col("left.users") < 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") < 10).leaveNRC()
      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - Integer GreaterThan Literal") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val expected = df.alias("left").join(df.alias("right"), col("left.users") > 10)
      val res = wrappedDf.join(wrappedDf, wrappedDf("users") > 10).leaveNRC()
      assertDataFrameEquals(res, expected)
    }

    it("Successful Self Join - String Literal Equality") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.join(df, df("language") === "Go")
      val res = wrappedDf.join(wrappedDf, wrappedDf("language") === "Go").leaveNRC()

      assertDataFrameEquals(res, expected)
    }

    it("Successful Join - 2 Flat Datasets on Non Distinct LessThan Column") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("lng", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("users") < df2("users"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") < wrappedDf2("users")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 2 Flat Datasets on Distinct LessThan Columns with other Non Distinct Columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "usr")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("users") < df2("usr"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") < wrappedDf2("usr")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 2 Flat Datasets on Distinct LessThanOrEqual Columns with other Non distinct columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "usr")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("users") <= df2("usr"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") <= wrappedDf2("usr")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }


    it("Successful Join - 3 Flat Datasets with Distinct Columns") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe2
      val df3 = simpleIntDataframe3

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, df("users") === df2("usr")).join(df3, df("users") === df3("userNo"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") === wrappedDf2("usr")).join(wrappedDf3, wrappedDf("users") === wrappedDf3("userNo")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 3 Flat Datasets with Distinct Join Column and other Non Distinct Columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("lName", "users")
      val df3 = data.toDF("lng", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, df("language") === df2("lName")).join(df3, df("language") === df3("lng"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("lName")).
        join(wrappedDf3, wrappedDf("language") === wrappedDf3("lng")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)

    }

    it("Successful Join - 3 Flat Datasets with 2 Non Distinct Columns in Equality Condition") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe4
      val df3 = simpleIntDataframe3

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, df("users") > df2("users")).join(df3, df("users") === df3("userNo"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") > wrappedDf2("users")).join(wrappedDf3, wrappedDf("users") === wrappedDf3("userNo")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }
    it("Successful Self Join - 3 Flat Datasets with 2 Non Distinct Columns in Equality Condition") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "usr")
      val df3 = data.toDF("language", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, df("users") === df2("usr")).join(df3, df("users") === df3("users"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") === wrappedDf2("usr")).join(wrappedDf3, wrappedDf("users") === wrappedDf3("users")).leaveNRC()
      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 3 Flat Dataframes with 2 Non Distinct Columns in GreaterThan and Equality Condition (first and last Condition have duplicate column names)") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe3
      val df3 = simpleIntDataframe4

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, df("users") > df2("userNo")).join(df3, df("users") === df3("users"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("users") > wrappedDf2("userNo")).join(wrappedDf3, wrappedDf("users") === wrappedDf3("users")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 4 Flat Datasets with Distinct Join Columns and other Non Distinct Columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("lName", "users")
      val df3 = data.toDF("lng", "users")
      val df4 = data.toDF("l", "u")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()
      val wrappedDf4 = df4.wrap()

      val expected = df.join(df2, df("language") === df2("lName")).join(df3, df("language") === df3("lng"))
        .join(df4, df("language") === df4("l"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("lName")).
        join(wrappedDf3, wrappedDf("language") === wrappedDf3("lng"))
        .join(wrappedDf4, wrappedDf("language") === wrappedDf4("l")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 4 Flat Datasets with Non Distinct Join Column and other Non Distinct (3 identical column names)") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "users")
      val df3 = data.toDF("lng", "users")
      val df4 = data.toDF("language", "u")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()
      val wrappedDf4 = df4.wrap()

      val expected = df.join(df2, df("language") === df2("language")).join(df3, df("language") === df3("lng"))
        .join(df4, df("language") === df4("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).
        join(wrappedDf3, wrappedDf("language") === wrappedDf3("lng"))
        .join(wrappedDf4, wrappedDf("language") === wrappedDf4("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 2 Nested Datasets, Top Level Join") {
      val df = nestedDataframe
      val df2 = nestedDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - 2 Multi-Nested Datasets, Top Level Join") {
      val df = multiNestedDataframe
      val df2 = multiNestedDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Join - Flat & Nested Datasets, Top Level Join") {
      val df2 = nestedDataframe
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Join with multi-nested structure & flat, top level join") {
      val df = simpleIntDataframe
      val df2 = multiNestedDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }
    it("Join with multi-nested structure & nested, top level join") {
      val df = nestedDataframe
      val df2 = multiNestedDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Join with multi-nested structure & nested, top level join with non distinct bag columns") {
      val df = nestedDataframe2
      val df2 = multiNestedDataframe
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Join with array type") {
      val df = arrayTypeDataframe
      val df2 = arrayTypeDataframe

      val wrappedDf = df.wrap()
      val wrappedDf2 = df.wrap()

      val expected = df.join(df2, df("name") === df2("name"))
      expected.show()

      val res = wrappedDf.join(wrappedDf2, wrappedDf("name") === wrappedDf2("name")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - String Condition Join, 2 Flat Datasets") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, "language")
      expected.show()

      val res = wrappedDf.join(wrappedDf2, "language").leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - String Condition Join, 3 Flat Datasets") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "users")
      val df3 = data.toDF("language", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, "language").join(df3, "language")
      expected.show()

      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, "language").leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - Seq Condition Join Single String, 2 Flat Datasets") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, Seq("language"))
      expected.show()

      val res = wrappedDf.join(wrappedDf2, Seq("language")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - Seq Condition Join 2 Strings, 2 Flat Datasets") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "users")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, Seq("language", "users"))
      expected.show()

      val res = wrappedDf.join(wrappedDf2, Seq("language", "users")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - Seq Condition Join Multi String, 3 Flat Datasets") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe4
      val df3 = simpleIntDataframe5


      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, "language").join(df3, Seq("language", "users"))
      expected.show()

      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, Seq("language", "users")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }


    //This fails can CMP bags currently
    it("Successful Join - Seq Condition Equi-Join Multi String including StructType column, 3 Nested Datasets") {
      val df = nestedDataframe2
      val df2 = multiNestedDataframe
      val df3 = nestedDataframe2

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, "language").join(df3, Seq("language", "stats"))
      expected.show()

      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, Seq("language", "stats")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }


    // This fails. We are comparing stats wchich is a nested type. NRC only supports PrimitiveCmp?
    it("Successful Join - StructType column condition, 2 Nested Datasets") {
      val df = nestedDataframe2
      val df3 = nestedDataframe3


      val wrappedDf = df.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df3, df("stats") === df3("stats"))
      expected.show()

      val res = wrappedDf.join(wrappedDf3, wrappedDf("stats") === wrappedDf3("stats")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - No Join Cond, 2 Flat Datasets") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe2

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2)
      val res = wrappedDf.join(wrappedDf2).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)

    }

    it("Successful Join - No Join Cond, Flat & Multi-Nested Datasets") {
      val df = simpleIntDataframe
      val df2 = multiNestedDataframe

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2)
      expected.show()
      val res = wrappedDf.join(wrappedDf2).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)

    }

    // TODO - nested join condition?
  }

  describe("Drop Duplicates") {
    it("Successful Drop Duplicates - No Duplicates") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.dropDuplicates()
      val res = wrappedDf.dropDuplicates.leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Drop Duplicates - Some Duplicates") {
      val df = simpleIntDataframe5
      val df2 = simpleIntDataframe5
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.union(df2).dropDuplicates()
      val res = wrappedDf.union(wrappedDf2).dropDuplicates.leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Unsuccessful Drop Duplicates - Inequality of Results") {
      val df = simpleIntDataframe5
      val df2 = simpleIntDataframe5
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.union(df2)
      val res = wrappedDf.union(wrappedDf2).dropDuplicates.leaveNRC()

      val caughtException = intercept[TestFailedException] {
        assertDataFramesAreEquivalent(expected, res)
      }

      assert(caughtException.isInstanceOf[TestFailedException])
    }
  }

  describe("Select") {
    it("Successful Select - Single String Column Flat Dataset") {
      val df = simpleIntDataframe

      val wrappedDf = df.wrap()

      val expected = df.select(df("language"))
      val res = wrappedDf.select(wrappedDf("language")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Single Int Column Flat Dataset") {
      val df = simpleIntDataframe

      val wrappedDf = df.wrap()

      val expected = df.select(df("users"))
      val res = wrappedDf.select(wrappedDf("users")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Multi value and type Column Flat Dataset") {
      val df = simpleAllTypesDataframe

      val wrappedDf = df.wrap()

      val expected = df.select(df("users"), df("weight"), df("language"), df("inUse"), df("percentage"))
      val res = wrappedDf.select(wrappedDf("users"), wrappedDf("weight"), wrappedDf("language"), wrappedDf("inUse"), wrappedDf("percentage")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }


  }


}


