package uk.ac.ox.cs.trance


import org.scalatest.{BeforeAndAfterEach, FunSpec}
import org.apache.spark.sql.DataFrame
import uk.ac.ox.cs.trance.app.TestApp.spark
import Wrapper.DataFrameImplicit
import utilities.{JoinCondContext, Symbol}
import org.apache.spark.sql.functions.col
import org.scalatest.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.TestDataframes._

class EndToEndScenarioTest extends FunSpec with BeforeAndAfterEach {


  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  def assertDataFramesAreEquivalent(df1: DataFrame, df2: DataFrame): Unit = {
    implicit val anyOrdering: Ordering[Any] = Ordering.fromLessThan {
      case (a, b) => a.toString < b.toString
    }

    val set1 = df1.collect().map(_.toSeq.sorted).toSet
    val set2 = df2.collect().map(_.toSeq.sorted).toSet

    set1 shouldEqual set2
  }


  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinCondContext.freshClear()

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


  }


}


