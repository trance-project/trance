package uk.ac.ox.cs.trance


import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, RelationalGroupedDataset, functions, Row => SparkRow}
import uk.ac.ox.cs.trance.app.TestApp.spark
import Wrapper.{DataFrameImplicit, wrap}
import framework.common.IntType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import utilities.{JoinContext, Symbol, TestDataframes}
import org.apache.spark.sql.functions.{col, collect_list, exp, monotonically_increasing_id, struct}
import org.apache.spark.sql.types.{ArrayType, DataTypes, DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.tools.repIf
import uk.ac.ox.cs.trance.utilities.SkewDataset.DatasetOps
import uk.ac.ox.cs.trance.utilities.TestDataframes._
//import sparkutils.skew.SkewDataset._
import scala.reflect.ClassTag


case class Record81f38c97198a4c3d82c07bdf8cc21c89(c_name: String)

case class Recorddfd1279ee2b64495a6067f8c39892143(c_name: String, Customer_index: Long)

case class Record9e5847f902704689a1c0cbd58204e07c(l_orderkey: Int)

case class Recordb8662d38214c4eccbca05aff64ad7253(c_name: String, Customer_index: Long, l_orderkey: Option[Int])

case class Recordf844ee1edbc947c29fc12a7a1429ced2(c_name: String, c_orders: Seq[Record9e5847f902704689a1c0cbd58204e07c])

case class Record95de9901799643d99af0b2fc86f09fdf(o_orderdate: String, o_orderkey: Int)

case class Recordbb4c9b827da843bab38ab4b0cff71ad6(o_orderdate: String, o_orderkey: Int, Order_index: Long)

case class Recordcd5962544f3d4361a05c67f62a9ec845(l_quantity: Double, l_partkey: Int, l_orderkey: Int)

case class Recorddd2ed07dab354fba9c7ce4139a96cc43(o_orderdate: String, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int], o_orderkey: Int, l_orderkey: Option[Int])

case class Record95020c82d35644059c5c5b406b79f713(Order_index: Long, o_orderdate: String, l_partkey: Option[Int], l_quantity: Option[Double])

case class Record4948c766a27942df940a6f7f71f34218(o_orderdate: String, Order_index: Long)

case class Record6444f3ba076c4c409686bf6410417a86(l_partkey: Int, l_quantity: Double)

case class Record10598666558d460ab7cf36a3624f7fe2(Order_index: Long, o_orderdate: String, o_parts: Seq[Record6444f3ba076c4c409686bf6410417a86])

class EndToEndScenarioTest extends AnyFunSpec with BeforeAndAfterEach with Serializable {


  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  def assertDataFrameSchemaEqual(df1: DataFrame, df2: DataFrame): Unit = {
    val schema1 = df1.schema.fields.map(f => f.copy(nullable = false))
    val schema2 = df2.schema.fields.map(f => f.copy(nullable = false))

    schema1 shouldEqual schema2
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
    it("Successful FlatMap - Integer Column Addition, Sequence of Rows Output") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.flatMap { x =>
        val id = x.getInt(0)
        val users = x.getInt(1)
        Seq(
          SparkRow(id, users),
          SparkRow(id + 10, users + 10)
        )
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.flatMap { x =>
        val id = x.get(0)
        val users = x.get(1)
        RepSeq(
          RepRow(id, users),
          RepRow(id + 10, users + 10)
        )
      }.leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)

    }
   it("Successful FlatMap - Integer Column Addition, Sequence of 3 Rows Output") {
        val df = pureIntDataframe
        val wrappedDf = df.wrap()

        val expected = df.flatMap { x =>
          val id = x.getInt(0)
          val users = x.getInt(1)
          Seq(
            SparkRow(id, users),
            SparkRow(id + 10, users + 10),
            SparkRow(id + 20, users + 20)
          )
        }(RowEncoder(df.schema))
        expected.show()

        val res = wrappedDf.flatMap { x =>
          val id = x.get(0)
          val users = x.get(1)
          RepSeq(
            RepRow(id, users),
            RepRow(id + 10, users + 10),
            RepRow(id + 20, users + 20)
          )
        }.leaveNRC()
        res.show()

        assertDataFramesAreEquivalent(res, expected)

      }

    it("Flat to Nested With Dummy Data") {
      val df = simpleIntDataframe3
      val df2 = simpleIntDataframe5


      val joinedDf = df.join(df2, df("lName") === df2("language"), "left_outer")
      val colum = functions.struct(joinedDf("users"), joinedDf("inUse"))
      val dfWithColumn = joinedDf.withColumn("nested", colum)
      val sparkResWithColumn = dfWithColumn.select("lName", "nested")

      sparkResWithColumn.show()


      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val res = wrappedDf.flatMap{f =>
          val lName = f.get(0)
          RepRow(lName, wrappedDf2.flatMap{ z =>
            val language = z.get(0)
            val users = z.get(1)
            val inUse = z.get(2)
            repIf(lName === language) {
              RepRow(users, inUse)
            } {
              RepRow()
            }
          })
      }.leaveNRC()

      res.show()


      assertDataFramesAreEquivalent(sparkResWithColumn, res)
    }
  }
  //TODO divide type promotion works differently in map
  describe("Map") {
    it("Successful Map - Identity Function") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map(x => x)(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map(x => x)(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    //TODO - how to handle renamed columns in TupleVarRef output of map
    it("Successful Map - Identity Function with Renamed columns") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()


      val schema: StructType = StructType(Seq(
        StructField("language", IntegerType, nullable = true),
        StructField("info", IntegerType, nullable = true
        )))


      val expected = df.map(x => x)(RowEncoder(schema))
      expected.show()

      val res = wrappedDf.map(x => x)(RepRowEncoder(schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)
    }

    it("Successful Map - Integer Column + 1") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + 1)
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>

        val k = x.toSeq.map(f => f + 1)

        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }


    it("Successful Map - Integer Column Multiples Additions") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + 1 + 2)
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f + 1 + 2)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }
    it("Successful Map - Integer Column Added to Itself") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int])
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f + f)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }
    it("Successful Map - Integer Column Added to Itself Twice") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int] + f.asInstanceOf[Int])
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f + f + f)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Map - Integer Column Added to Itself Multiple Times") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int] + f.asInstanceOf[Int] + 2 + f.asInstanceOf[Int] + 3 + f.asInstanceOf[Int])
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f + f + f + 2 + f + 3 + f)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Map - Integer Mod with brackets") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] % (f.asInstanceOf[Int] + 1))
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f % (f + 1))
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Successful Map - Integer Column Minus to Itself") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] - f.asInstanceOf[Int] - f.asInstanceOf[Int])
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f - f - f)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    // TODO - type promotion for double shouldn't happen here
    it("Successful Map - Integer All Operations Tuple Both Sides Issue") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] - f.asInstanceOf[Int] + f.asInstanceOf[Int] * f.asInstanceOf[Int] / f.asInstanceOf[Int] % f.asInstanceOf[Int])
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map(f => f - f + f * f / f % f)
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }
    // TODO - Use UDF?
    it("Successful Map - String Column Modification") {
      val df = pureStringDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        SparkRow.fromSeq(x.toSeq.map(f => s"${f.asInstanceOf[String]}: Test"))
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>
        val k = x.toSeq.map { f =>
          val k = f + ": Test"
          k
        }
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show(false)

      assertDataFramesAreEquivalent(res, expected)
    }
    //
    //it("Successful Map - String Column Modification Alternate Way") {
    //      val df = pureStringDataframe
    //      val wrappedDf = df.wrap()
    //
    //      val expected = df.map { x =>
    //        SparkRow.fromSeq(x.toSeq.map(f => "Replaced"))
    //      }(RowEncoder(df.schema))
    //      expected.show()
    //
    //      import uk.ac.ox.cs.trance.repextensions._
    //      val res = wrappedDf.map{ x =>
    //       val k = x.toSeq.tMap(x => "Replaced")
    //        RepRow.fromSeq(k)
    //      }(RepRowEncoder(df.schema)).leaveNRC()
    //      res.show(false)
    //
    //      assertDataFramesAreEquivalent(res, expected)
    //    }

    it("moreTesting - conditional if in integer addition mapping") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val out = df.map { x =>
        val k = x.toSeq.map { f =>
          if (f.asInstanceOf[Int] > 50) {
            f.asInstanceOf[Int] + 29
          } else {
            f
          }
        }
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))

      out.show()


      val res = wrappedDf.map { x =>
        println("RepRow: " + x)

        val k = x.toSeq.map { f =>
          repIf(f > 50) {
            f + 29
          } {
            f
          }
        }
        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()

      res.show()
      assertDataFramesAreEquivalent(res, out)
      assertDataFrameSchemaEqual(res, out)


    }

    it("Map change Integer output columns in RowEncoder") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val schema: StructType = StructType(Seq(
        StructField("language", IntegerType, nullable = true),
        StructField("info", IntegerType, nullable = true
        )))

      val expected = df.map { f =>
        val k = Seq(f.getInt(1), f.getInt(0))
        SparkRow.fromSeq(k)
      }(RowEncoder(schema))

      val res = wrappedDf.map { f =>
        val k = Seq(f.get(1), f.get(0))
        RepRow.fromSeq(k)
      }(RepRowEncoder(schema)).leaveNRC()

      expected.show()
      res.show()
      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)


    }
    it("Map change String output columns in RowEncoder") {
      val df = pureStringDataframe
      val wrappedDf = df.wrap()

      val schema: StructType = StructType(Seq(
        StructField("language", StringType, nullable = true),
        StructField("info", StringType, nullable = true
        )))

      val expected = df.map { f =>
        val k = Seq(f.getString(1), f.getString(0))
        SparkRow.fromSeq(k)
      }(RowEncoder(schema))

      val res = wrappedDf.map { f =>
        val k = Seq(f.get(1), f.get(0))
        RepRow.fromSeq(k)
      }(RepRowEncoder(schema)).leaveNRC()

      expected.show()
      res.show()
      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)


    }
    it("Change Same Number of Column Names in Map") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val schema: StructType = StructType(Seq(
        StructField("column1", IntegerType, nullable = true),
        StructField("column2", IntegerType, nullable = true
        )))


      val expected = df.map { f => SparkRow(f.get(0), f.get(1)) }(RowEncoder(schema))

      expected.show()
      expected.printSchema()

      val res = wrappedDf.map { f => RepRow(f.get(0), f.get(1)) }(RepRowEncoder(schema)).leaveNRC()

      res.show()
      res.printSchema()

      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)
    }

    it("Map change Int to Double with Arithmetic Expression - Same Column Names") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val schema: StructType = StructType(Seq(
        StructField("column1", DoubleType, nullable = true),
        StructField("column2", DoubleType, nullable = true
        )))

      val expected = df.map { f =>
        val column1 = f.getInt(0) * 2.5
        val column2 = f.getInt(1) * 1.5
        SparkRow(column1, column2)
      }(RowEncoder(schema))

      expected.show()
      expected.printSchema()

      val res = wrappedDf.map { f =>
        val column1 = f.get(0) * 2.5
        val column2 = f.get(1) * 1.5
        RepRow(column1, column2)
      }(RepRowEncoder(schema)).leaveNRC()

      res.show()
      res.printSchema()

      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)
    }
    it("Map change Int output types to String in RowEncoder different column names") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val schema: StructType = StructType(Seq(
        StructField("helloColumn", StringType, nullable = true),
        StructField("users", DoubleType, nullable = true
        )))

      val expected = df.map { f =>
        val column1 = "Hello"
        val column2 = f.getInt(1) * 1.5
        SparkRow(column1, column2)
      }(RowEncoder(schema))

      expected.show()
      expected.printSchema()

      val res = wrappedDf.map { f =>
        val column1 = Alias(f.get(0), "Hello")
        val column2 = f.get(1) * 1.5
        RepRow(column1, column2)
      }(RepRowEncoder(schema)).leaveNRC()

      res.show()
      res.printSchema()

      assertDataFramesAreEquivalent(res, expected)
      assertDataFrameSchemaEqual(res, expected)
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

    // TODO - Fix test
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

    it("Successful Join - 2 Flat Datasets on Distinct GreaterThanEqual Columns with other Non distinct columns") {
      val data: Seq[(String, Int)] = Seq(("Go", 20), ("Ruby", 90), ("Rust", 100), ("Go", 10))
      import spark.implicits._
      val df = data.toDF("language", "users")
      val df2 = data.toDF("language", "usr")

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df2("usr") >= df("users"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf2("usr") >= wrappedDf("users")).leaveNRC()

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
      expected.show()
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()
      res.show()

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

    it("Join with array type - 2 Flat datasets String Join Condition") {
      val df = arrayTypeDataframe
      val df2 = arrayTypeDataframe

      val wrappedDf = df.wrap()
      val wrappedDf2 = df.wrap()

      val expected = df.join(df2, df("name") === df2("name"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("name") === wrappedDf2("name")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Join with array type - 2 Flat datasets Array Join Condition") {
      val df = arrayTypeDataframe
      val df2 = arrayTypeDataframe

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Join with array type - 2 Flat datasets Array Join Condition, 1/3 matches") {
      val df = arrayTypeDataframe
      val df2 = arrayTypeDataframe2

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.join(df2, df("language") === df2("language"))
      val res = wrappedDf.join(wrappedDf2, wrappedDf("language") === wrappedDf2("language")).leaveNRC()

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
      val res = wrappedDf.join(wrappedDf2, "language").leaveNRC()


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
      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, "language").leaveNRC()


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
      val res = wrappedDf.join(wrappedDf2, Seq("language")).leaveNRC()


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
      val res = wrappedDf.join(wrappedDf2, Seq("language", "users")).leaveNRC()

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
      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, Seq("language", "users")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - Seq Condition Equi-Join Multi String including StructType column, 3 Nested Datasets") {
      val df = nestedDataframe2
      val df2 = multiNestedDataframe
      val df3 = nestedDataframe2

      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df2, "language").join(df3, Seq("language", "stats"))
      val res = wrappedDf.join(wrappedDf2, "language").join(wrappedDf3, Seq("language", "stats")).leaveNRC()


      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - StructType Column Condition, 2 Nested Datasets") {
      val df = nestedDataframe2
      val df3 = nestedDataframe3

      val wrappedDf = df.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df3, df("stats") === df3("stats"))
      val res = wrappedDf.join(wrappedDf3, wrappedDf("stats") === wrappedDf3("stats")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - StructType Column Condition, 2 Nested Datasets 1/3 Matched") {
      val df = nestedDataframe2
      val df3 = nestedDataframe4


      val wrappedDf = df.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df3, df("stats") === df3("stats"))
      val res = wrappedDf.join(wrappedDf3, wrappedDf("stats") === wrappedDf3("stats")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Join - StructType Column Condition, Empty Result Set") {
      val df = nestedDataframe2
      val df3 = nestedDataframe5


      val wrappedDf = df.wrap()
      val wrappedDf3 = df3.wrap()

      val expected = df.join(df3, df("stats") === df3("stats"))
      val res = wrappedDf.join(wrappedDf3, wrappedDf("stats") === wrappedDf3("stats")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Unsuccessful Join - StructType Column Condition, Different Nested Types") {
      val df = nestedDataframe
      val df2 = multiNestedDataframe


      val wrappedDf = df.wrap()
      val wrappedDf3 = df2.wrap()

      val caughtException = intercept[Throwable] {
        wrappedDf.join(wrappedDf3, wrappedDf("info") === wrappedDf3("stats")).leaveNRC()
      }

      assert(caughtException.isInstanceOf[AssertionError])

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
      val res = wrappedDf.join(wrappedDf2).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)

    }

    // TODO - nested join condition?
  }

  describe("Drop Duplicates") {
    it("Successful Drop Duplicates - No Duplicates") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.dropDuplicates()
      val res = wrappedDf.dropDuplicates().leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Drop Duplicates - Some Duplicates") {
      val df = simpleIntDataframe5
      val df2 = simpleIntDataframe5
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.union(df2).dropDuplicates()
      val res = wrappedDf.union(wrappedDf2).dropDuplicates().leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Unsuccessful Drop Duplicates - Inequality of Results") {
      val df = simpleIntDataframe5
      val df2 = simpleIntDataframe5
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val expected = df.union(df2)
      val res = wrappedDf.union(wrappedDf2).dropDuplicates().leaveNRC()

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

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Single String Flat Dataset") {
      val df = simpleIntDataframe

      val wrappedDf = df.wrap()

      val expected = df.select("language")
      val res = wrappedDf.select("language").leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Multiple String Flat Dataset") {
      val df = simpleIntDataframe

      val wrappedDf = df.wrap()

      val expected = df.select("language", "users")
      val res = wrappedDf.select("language", "users").leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Single Int Column Flat Dataset") {
      val df = simpleIntDataframe

      val wrappedDf = df.wrap()

      val expected = df.select(df("users"))
      val res = wrappedDf.select(wrappedDf("users")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Multi value and type Column Flat Dataset") {
      val df = simpleAllTypesDataframe

      val wrappedDf = df.wrap()

      val expected = df.select(df("users"), df("weight"), df("language"), df("inUse"), df("percentage"))
      val res = wrappedDf.select(wrappedDf("users"), wrappedDf("weight"), wrappedDf("language"), wrappedDf("inUse"), wrappedDf("percentage")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Select on Select Result") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users"), df("language")).select(df("users"))
      val res = wrappedDf.select(wrappedDf("users"), wrappedDf("language")).select(wrappedDf("users")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression Integer Equality") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") === 20)
      val res = wrappedDf.select(wrappedDf("users") === 20).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression Integer GreaterThan") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") > 20)
      val res = wrappedDf.select(wrappedDf("users") > 20).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression Integer GreaterThanOrEqual") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") >= 20)
      val res = wrappedDf.select(wrappedDf("users") >= 20).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression Integer LessThan") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") < 20)
      val res = wrappedDf.select(wrappedDf("users") < 20).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression Integer LessThanOrEqual") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") <= 20)
      val res = wrappedDf.select(wrappedDf("users") <= 20).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Boolean Expression String Equality") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("language") === "Go")
      val res = wrappedDf.select(wrappedDf("language") === "Go").leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    // TODO - Check if want this not to be an assertion failure in NRC

    it("Successful Select - Unions ") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") > 10).union(df.select(df("users") >= 2))
      expected.show()

      val res = wrappedDf.select(wrappedDf("users") > 10).union(wrappedDf.select(wrappedDf("users") >= 20)).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }


    it("Successful Select - Select Multiply Int/Int Unnamed, Flat Dataset") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") * df("users"))
      val res = wrappedDf.select(wrappedDf("users") * wrappedDf("users")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Select Multiple Math Operators Unnamed, Flat Dataset All Integer Result") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") * df("users") - df("users") / df("users") + df("users") % df("users"))
      val res = wrappedDf.select(wrappedDf("users") * wrappedDf("users") - wrappedDf("users") / wrappedDf("users") + wrappedDf("users") % wrappedDf("users")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Select Multiply All Numerical Types Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") * df("weight"), df("users") * df("percentage"),
        df("percentage") * df("users"), df("percentage") * df("percentage"), df("percentage") * df("weight"),
        df("weight") * df("users"), df("weight") * df("percentage"), df("weight") * df("weight"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") * wrappedDf("weight"), wrappedDf("users") * wrappedDf("percentage"),
        wrappedDf("percentage") * wrappedDf("users"), wrappedDf("percentage") * wrappedDf("percentage"), wrappedDf("percentage") * wrappedDf("weight"),
        wrappedDf("weight") * wrappedDf("users"), wrappedDf("weight") * wrappedDf("percentage"), wrappedDf("weight") * wrappedDf("weight")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Select Add All Numerical Types Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") + df("weight"), df("users") + df("percentage"),
        df("percentage") + df("users"), df("percentage") + df("percentage"), df("percentage") + df("weight"),
        df("weight") + df("users"), df("weight") + df("percentage"), df("weight") + df("weight"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") + wrappedDf("weight"), wrappedDf("users") + wrappedDf("percentage"),
        wrappedDf("percentage") + wrappedDf("users"), wrappedDf("percentage") + wrappedDf("percentage"), wrappedDf("percentage") + wrappedDf("weight"),
        wrappedDf("weight") + wrappedDf("users"), wrappedDf("weight") + wrappedDf("percentage"), wrappedDf("weight") + wrappedDf("weight")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Select Sub All Numerical Types Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") - df("weight"), df("users") - df("percentage"),
        df("percentage") - df("users"), df("percentage") - df("percentage"), df("percentage") - df("weight"),
        df("weight") - df("users"), df("weight") - df("percentage"), df("weight") - df("weight"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") - wrappedDf("weight"), wrappedDf("users") - wrappedDf("percentage"),
        wrappedDf("percentage") - wrappedDf("users"), wrappedDf("percentage") - wrappedDf("percentage"), wrappedDf("percentage") - wrappedDf("weight"),
        wrappedDf("weight") - wrappedDf("users"), wrappedDf("weight") - wrappedDf("percentage"), wrappedDf("weight") - wrappedDf("weight")).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }


    it("Successful Select - Select Divide All Numerical Types Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") / df("weight"), df("users") / df("percentage"),
        df("percentage") / df("users"), df("percentage") / df("percentage"), df("percentage") / df("weight"),
        df("weight") / df("users"), df("weight") / df("percentage"), df("weight") / df("weight"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") / wrappedDf("weight"), wrappedDf("users") / wrappedDf("percentage"),
        wrappedDf("percentage") / wrappedDf("users"), wrappedDf("percentage") / wrappedDf("percentage"), wrappedDf("percentage") / wrappedDf("weight"),
        wrappedDf("weight") / wrappedDf("users"), wrappedDf("weight") / wrappedDf("percentage"), wrappedDf("weight") / wrappedDf("weight")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Select Divide Long & IntTypes Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") / df("percentage"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") / wrappedDf("percentage")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Select Mod All Numerical Types Unnamed, Flat Dataset") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") % df("weight"), df("users") % df("percentage"),
        df("percentage") % df("users"), df("percentage") % df("percentage"), df("percentage") % df("weight"),
        df("weight") % df("users"), df("weight") % df("percentage"), df("weight") % df("weight"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") % wrappedDf("weight"), wrappedDf("users") % wrappedDf("percentage"),
        wrappedDf("percentage") % wrappedDf("users"), wrappedDf("percentage") % wrappedDf("percentage"), wrappedDf("percentage") % wrappedDf("weight"),
        wrappedDf("weight") % wrappedDf("users"), wrappedDf("weight") % wrappedDf("percentage"), wrappedDf("weight") % wrappedDf("weight")).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Multiply long column by Long literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") * 25L)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") * 25L).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Add double column with Double literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("weight") + 2.4)
      expected.show()
      val res = wrappedDf.select(wrappedDf("weight") + 2.4).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - Subtract Integer Column with Long Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") - 1000L)
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") - 1000L).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Divide Long Column with Integer Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") / 19)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") / 19).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Divide Int Column with Integer Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("users") / df("users"))
      expected.show()
      val res = wrappedDf.select(wrappedDf("users") / wrappedDf("users")).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Divide Long Column with Long Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") / 19L)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") / 19L).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Mod Double Column with Long Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("weight") % 19L)
      expected.show()
      val res = wrappedDf.select(wrappedDf("weight") % 19L).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - Mod Long Column with Long Literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") % 19L)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") % 19L).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Select - OR column by literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") === 75L || df("weight") === 2.5)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") === 75L || wrappedDf("weight") === 2.5).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful Select - AND column by literal") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.select(df("percentage") === 75L && df("weight") === 7.5)
      expected.show()
      val res = wrappedDf.select(wrappedDf("percentage") === 75L && wrappedDf("weight") === 7.5).leaveNRC()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Succesful select - Primitive value from nested Dataset") {
      val df = nestedDataframe
      val wrappedDf = df.wrap()

      val expected = df.select("language")
      val res = wrappedDf.select("language").leaveNRC()

      assertDataFramesAreEquivalent(expected, res)

    }
    it("Succesful select - Struct value from nested Dataset") {
      val df = nestedDataframe
      val wrappedDf = df.wrap()

      val expected = df.select("info")
      expected.show()
      val res = wrappedDf.select("info").leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)

    }
    it("Succesful select - Struct value from nested Select struct value") {
      val df = simplerMultiNestedDataframe
      val wrappedDf = df.wrap()

      val expected = df.select("stats")
      expected.show()
      val res = wrappedDf.select("stats").leaveNRC()


      res.show()
      assertDataFramesAreEquivalent(expected, res)

    }
  }

  describe("GroupBy") {
    it("Successful GroupBy String Column and Sum Integer Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.groupBy("language").sum("users")
      expected.show()

      val res = wrappedDf.groupBy("language").sum("users").leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }
    it("Successful GroupBy Integer Column and Sum Integer Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.groupBy("users").sum("users")
      expected.show()

      val res = wrappedDf.groupBy("users").sum("users").leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(expected, res)
    }

    it("Unsuccessful GroupBy - Sum on Non Numerical Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val caughtException = intercept[Throwable] {
        wrappedDf.groupBy("users").sum("language").leaveNRC()
      }

      assert(caughtException.isInstanceOf[AssertionError])
    }


  }

  describe("Filter") {
    it("Successful Filter - Equality on String Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("language") === "Go")
      expected.show()
      val res = wrappedDf.filter(wrappedDf("language") === "Go").leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - Equality on Integer Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("users") === 20)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("users") === 20).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - LTE on Integer Column") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("users") <= 20)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("users") <= 20).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - Not Equal on Long Column") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("percentage") =!= 75L)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("percentage") =!= 75L).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }


    it("Successful Filter - Or and Not Equal on Long Column") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("percentage") =!= 75L || df("weight") < 25.0)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("percentage") =!= 75L || wrappedDf("weight") < 25.0).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - And and Not Equal on Double Column") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("percentage") =!= 75L && df("users") > 200)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("percentage") =!= 75L && wrappedDf("users") > 200).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - Filter on Filter Result") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.filter(df("percentage") =!= 75L).filter(df("users") > 200)
      expected.show()
      val res = wrappedDf.filter(wrappedDf("percentage") =!= 75L).filter(wrappedDf("users") > 200).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }

    it("Successful Filter - Filter with Where Clause Result") {
      val df = simpleAllTypesDataframe
      val wrappedDf = df.wrap()

      val expected = df.where(df("percentage") =!= 75L).where(df("users") > 200)
      expected.show()
      val res = wrappedDf.where(wrappedDf("percentage") =!= 75L).where(wrappedDf("users") > 200).leaveNRC()
      res.show()
      assertDataFramesAreEquivalent(expected, res)
    }


  }
  // TODO - WIP
  describe("tpch tests") {
    it("Flat to Nested With Dummy Data") {
      val df = simpleIntDataframe3
      val df2 = simpleIntDataframe5


      val joinedDf = df.join(df2, df("lName") === df2("language"), "left_outer")
      val colum = functions.struct(joinedDf("users"), joinedDf("inUse"))
      val dfWithColumn = joinedDf.withColumn("nested", colum)
      val sparkResWithColumn = dfWithColumn.select("lName", "nested")

      sparkResWithColumn.show()


      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()

      val res = wrappedDf.flatMap { f =>
        val lName = f.get(0)
        RepRow(lName, wrappedDf2.flatMap { z =>
          val language = z.get(0)
          val users = z.get(1)
          val inUse = z.get(2)
          repIf(lName === language) {
            RepRow(users, inUse)
          } {
            RepRow()
          }
        })
      }.leaveNRC()

      res.show()


      assertDataFramesAreEquivalent(sparkResWithColumn, res)
    }


    it("FlatToNested - Test1") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val oquery = wrappedOrder.flatMap { f =>
        val o_orderdate = f.get(4)
        val o_orderkey = f.get(0)
        RepRow(o_orderdate, wrappedLineItem.flatMap { z =>
          val l_orderkey = z.get(0)
          val l_partkey = z.get(1)
          val l_quantity = z.get(4)
          repIf(l_orderkey === o_orderkey) {
            RepRow(l_partkey, l_quantity)
          } {
            RepRow.empty
          }
        }.as("o_parts"))
      }.leaveNRC()

      oquery.show()

    }
  }
  it("FlatToNested Test1Full") {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()


    val oquery = wrappedOrder.flatMap { f =>
      val o_orderkey = f.get(0)
      RepRow(f, wrappedLineItem.flatMap { z =>
        val l_orderkey = z.get(0)
        repIf(l_orderkey === o_orderkey) {
          RepRow(z)
        } {
          RepRow.empty
        }
      })
    }.leaveNRC()

  oquery.show(false)

  }
  it("FlatToNested Test2") {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()

    val customer = Customer
    val wrappedCustomer = customer.wrap()

    val sparkRes: Dataset[SparkRow] = customer
      .join(order, col("c_custkey") === col("o_custkey"))
      .join(lineItem, col("l_orderkey") === col("o_orderkey"))
      .select(col("c_name"), col("o_orderdate"), col("l_partkey"), col("l_quantity"))

    sparkRes.show()

    val wrappedRes = wrappedCustomer
      .join(wrappedOrder, wrappedCustomer("c_custkey") === wrappedOrder("o_custkey"))
      .join(wrappedLineItem, wrappedOrder("o_orderkey") === wrappedLineItem("l_orderkey"))
      .select("c_name", "o_orderdate", "l_partkey", "l_quantity").leaveNRC()
    wrappedRes.show()

    assertDataFramesAreEquivalent(sparkRes, wrappedRes)

  }
  it("FlatToNested Test2Filter") {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()

    val customer = Customer
    val wrappedCustomer = customer.wrap()

    val sparkRes: Dataset[SparkRow] = customer
      .join(order, col("c_custkey") === col("o_custkey"))
      .join(lineItem, col("l_orderkey") === col("o_orderkey"))
      .filter(col("c_nationkey") === 1)
      .select(col("c_name"), col("o_orderdate"), col("l_partkey"), col("l_quantity"))

    val wrappedRes = wrappedCustomer
      .join(wrappedOrder, wrappedCustomer("c_custkey") === wrappedOrder("o_custkey"))
      .join(wrappedLineItem, wrappedOrder("o_orderkey") === wrappedLineItem("l_orderkey"))
      .filter(wrappedCustomer("c_nationkey") === 1)
      .select("c_name", "o_orderdate", "l_partkey", "l_quantity").leaveNRC()
    wrappedRes.show()

    assertDataFramesAreEquivalent(sparkRes, wrappedRes)

  }

  // TODO - WIP
  // Corresponds to TPCH 'oquery' from compiler examples/tpch/FlatToNested.scala line 117
  it("FlatToNested Test2Flat") {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()

    val customer = Customer
    val wrappedCustomer = customer.wrap()

    val outputSchema = StructType(Seq(
      StructField("o_custkey", IntegerType),
      StructField("o_orderdate", StringType),
      StructField("o_parts", StructType(Seq(
        StructField("l_partkey", IntegerType),
        StructField("l_quantity", DoubleType)
      )))
    ))

    val oquery = wrappedOrder.map { f =>
      val o_custkey = f.get(1)
      val o_orderdate = f.get(4)

      val o_parts: WrappedDataframe = wrappedOrder
        .join(wrappedLineItem, wrappedOrder("o_orderkey") === wrappedLineItem("l_orderkey"))
        .select("l_partkey", "l_quantity")

      val out = o_parts.flatMap {
        z => RepSeq(RepRow(o_custkey, o_orderdate, z))
      }

      out
    }(RepRowEncoder(outputSchema)).leaveNRC()

    oquery.show()

  }

  describe("Demo Tests") {
    it("Successful FlatMap - Integer Column + 1") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.flatMap { x =>
        val id = x.getInt(0)
        val users = x.getInt(1)
        Seq(
          SparkRow(id, users),
          SparkRow(id + 10, users + 10)
        )
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.flatMap { x =>
        val id = x.get(0)
        val users = x.get(1)
        RepSeq(
          RepRow(id, users),
          RepRow(id + 10, users + 10)
        )
      }.leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)

    }


    it("Map - Integer Column + 1") {
      val df = pureIntDataframe
      val wrappedDf = df.wrap()

      val expected = df.map { x =>
        val k = x.toSeq.map(f => f.asInstanceOf[Int] + 1)
        SparkRow.fromSeq(k)
      }(RowEncoder(df.schema))
      expected.show()

      val res = wrappedDf.map { x =>

        val k = x.toSeq.map(f => f + 1)

        RepRow.fromSeq(k)
      }(RepRowEncoder(df.schema)).leaveNRC()
      res.show()

      assertDataFramesAreEquivalent(res, expected)
    }

    it("Generated Code Test") {
      import spark.implicits._

      val Lineitem = LineItem
      Lineitem.cache
      Lineitem.count

      val Customer = TestDataframes.Customer
      Customer.cache
      Customer.count

      val x15 = Customer.select("c_name")

        .as[Record81f38c97198a4c3d82c07bdf8cc21c89]

      val x16 = x15.withColumn("Customer_index", monotonically_increasing_id())
        .as[Recorddfd1279ee2b64495a6067f8c39892143]

      val x18 = Lineitem.select("l_orderkey")

        .as[Record9e5847f902704689a1c0cbd58204e07c]

      val x21 = x16.crossJoin(x18)
        .as[Recordb8662d38214c4eccbca05aff64ad7253]

      val x23 = x21


      val x25 = x23.groupByKey(x24 => Recorddfd1279ee2b64495a6067f8c39892143(x24.c_name, x24.Customer_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x24 =>
            (x24.l_orderkey) match {
              case (None) => Seq()
              case _ => Seq(Record9e5847f902704689a1c0cbd58204e07c(x24.l_orderkey match { case Some(x) => x; case _ => 0 }))
            }).toSeq
          Recordf844ee1edbc947c29fc12a7a1429ced2(key.c_name, grp)
      }.as[Recordf844ee1edbc947c29fc12a7a1429ced2]

      val x26 = x25
      val TestX = x26
      TestX.show()
      TestX.printSchema()
      //TestX.count

    }

    it("Flat to Nested Generated Test 1") {

      val Lineitem = LineItem
      Lineitem.cache
      Lineitem.count
      val order = Order
      order.cache
      order.count

      import spark.implicits._


      val x13 = order.select("o_orderdate", "o_orderkey")
        .as[Record95de9901799643d99af0b2fc86f09fdf]

      val x14 = x13.withColumn("Order_index", monotonically_increasing_id())
        .as[Recordbb4c9b827da843bab38ab4b0cff71ad6]

      val x16 = Lineitem.select("l_quantity", "l_partkey", "l_orderkey")

        .as[Recordcd5962544f3d4361a05c67f62a9ec845]


      val x19 = x14.equiJoin(x16, Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recorddd2ed07dab354fba9c7ce4139a96cc43]

      val x21 = x19.select("Order_index", "o_orderdate", "l_partkey", "l_quantity")

        .as[Record95020c82d35644059c5c5b406b79f713]

      val x23 = x21.groupByKey(x22 => Record4948c766a27942df940a6f7f71f34218(x22.o_orderdate, x22.Order_index)).mapGroups {
        case (key, value) =>
          val grp = value.flatMap(x22 =>
            (x22.l_partkey, x22.l_quantity) match {
              case (None, _) => Seq()
              case (_, None) => Seq()
              case _ => Seq(Record6444f3ba076c4c409686bf6410417a86(x22.l_partkey match { case Some(x) => x; case _ => 0 }, x22.l_quantity match { case Some(x) => x; case _ => 0.0 }))
            }).toSeq
          Record10598666558d460ab7cf36a3624f7fe2(key.Order_index, key.o_orderdate, grp)
      }.as[Record10598666558d460ab7cf36a3624f7fe2]

      val x24 = x23

      x24.show(false)
      x24.printSchema()
    }
  }
}


