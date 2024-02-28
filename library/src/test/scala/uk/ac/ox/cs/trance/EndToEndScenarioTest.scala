package uk.ac.ox.cs.trance


import org.scalatest.BeforeAndAfterEach
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoders, RelationalGroupedDataset, functions, Row => SparkRow}
import uk.ac.ox.cs.trance.app.TestApp.spark
import Wrapper.DataFrameImplicit
import framework.common.IntType
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import utilities.{JoinContext, Symbol, TestDataframes}
import org.apache.spark.sql.functions.{col, collect_list, exp, first, monotonically_increasing_id, struct}
import org.apache.spark.sql.types.{ArrayType, DataTypes, DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.scalatest.exceptions.TestFailedException
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.SkewDataset.DatasetOps
import uk.ac.ox.cs.trance.utilities.TPCHDataframes.{ArrayCOP, ArrayCOP2Arrauys, COP, LineItem, MixedCOP}
import uk.ac.ox.cs.trance.utilities.TestDataframes._
//import uk.ac.ox.cs.trance.utilities.TPCHDataframes._
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

case class Record2dc37b6c982f4fc5864bfea8830b976e(c_name: String, c_nationkey: Int, c_custkey: Int)
case class Record99b6f24fbc634ee9b3639e57d1bf20c1(c_name: String, c_nationkey: Int, c_custkey: Int, Customer_index: Long)
case class Recorde3649c70440340cfad4b3323d08e2e3b(o_orderdate: String, o_custkey: Int, o_orderkey: Int)
case class Record6687cebc34a748a8bb09d5668686082e(o_orderdate: String, o_custkey: Int, o_orderkey: Int, Order_index: Long)
case class Recordd220fd669d7e41f1b86a3aabecac4237(o_orderdate: Option[String], o_custkey: Option[Int], Order_index: Option[Long], c_name: String, Customer_index: Long, c_nationkey: Int, c_custkey: Int, o_orderkey: Option[Int])
case class Record478e4064b2aa4a81882aa61634d044c9(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Record811a07786a1742d68b5b3e724eb7e675(o_orderdate: Option[String], l_quantity: Option[Double], Order_index: Option[Long], c_name: String, Customer_index: Long, l_partkey: Option[Int], o_orderkey: Option[Int], l_orderkey: Option[Int])
case class Record7b14754fea1d449e84f101370561bb95(o_orderdate: Option[String], l_quantity: Option[Double], Order_index: Option[Long], c_name: String, Customer_index: Long, l_partkey: Option[Int])
case class Record673d683609d24f698c9fab8a214ccb52(o_orderdate: Option[String], Order_index: Option[Long], c_name: String, Customer_index: Long)
case class Recorda14eb38fa0b14107a1e4e87ea2d35fde(l_partkey: Int, l_quantity: Double)
case class Recordbcc1e90432f548a2b9eb13a23cafb152(o_orderdate: Option[String], Order_index: Option[Long], c_name: String, o_parts: Seq[Recorda14eb38fa0b14107a1e4e87ea2d35fde], Customer_index: Long)
case class Record190f73c1f64d49a297297765c57c8bde(Customer_index: Long, c_name: String, o_orderdate: Option[String], o_parts: Seq[Recorda14eb38fa0b14107a1e4e87ea2d35fde])
case class Record5c1622d89e264a809d3f13e0d4a12ed8(c_name: String, Customer_index: Long)
case class Record77304327a217441fa8f29bdd4a041d34(o_orderdate: String, o_parts: Seq[Recorda14eb38fa0b14107a1e4e87ea2d35fde])
case class Record3df1924806c44fcdafc4395b39bd3b4c(Customer_index: Long, c_name: String, c_orders: Seq[Record77304327a217441fa8f29bdd4a041d34])

case class Recordb80e3302266a42e4a981836379570249(o_orderdate: String, o_custkey: Int, o_orderkey: Int)
case class Recordd16d0fb04d69452d83bec9ee88449806(o_orderdate: String, o_custkey: Int, o_orderkey: Int, Order_index: Long)
case class Record80e931fa827f40cca9ea94aa8b0a308a(l_quantity: Double, l_partkey: Int, l_orderkey: Int)
case class Recordbf846df439974b07b6abb5120e933eb8(o_orderdate: String, o_custkey: Int, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int], o_orderkey: Int, l_orderkey: Option[Int])
case class Recordd74a63caf68e4419868a969376dbf27b(o_orderdate: String, o_custkey: Int, l_quantity: Option[Double], Order_index: Long, l_partkey: Option[Int])
case class Recorde64677bfead1481e9fe09c808070d452(o_orderdate: String, o_custkey: Int, Order_index: Long)
case class Record0f24c31b203c447584d817d42bc7dd63(l_partkey: Int, l_quantity: Double)
case class Record07ff0f6ff4644ebeadc4697bd56a2d39(o_orderdate: String, o_custkey: Int, Order_index: Long, o_parts: Seq[Record0f24c31b203c447584d817d42bc7dd63])
case class Recordea3dc6f9b6ed4825a085a971a8423122(c_name: String, c_custkey: Int)
case class Record9f1664be1da24f109f1aea1ab3e17c27(c_name: String, c_custkey: Int, Customer_index: Long)
case class Record2fe5f740223c42fb8c010e46406e8f73(o_custkey: Int, o_orderdate: String, o_parts: Seq[Record0f24c31b203c447584d817d42bc7dd63])
case class Record53d1eaf1461b4a32adb18c8ef293ff5e(o_orderdate: Option[String], o_custkey: Option[Int], c_name: String, o_parts: Option[Seq[Record0f24c31b203c447584d817d42bc7dd63]], Customer_index: Long, c_custkey: Int)
case class Recordfbdaef1028e84a7f828a554c285e6e01(Customer_index: Long, c_name: String, o_orderdate: Option[String], o_parts: Option[Seq[Record0f24c31b203c447584d817d42bc7dd63]])
case class Record16e3a795c8c148c0a5a11f439e060620(c_name: String, Customer_index: Long)
case class Record93f64805c41d4615a2e3ea7d2dc10155(o_orderdate: String, o_parts: Seq[Record0f24c31b203c447584d817d42bc7dd63])
case class Record1b47e1a717b84648887c65f20c823179(Customer_index: Long, c_name: String, c_orders: Seq[Record93f64805c41d4615a2e3ea7d2dc10155])
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

  describe("New Map Tests") {
    it("Simple New Style Map 1") {
      val df = simpleIntDataframe
      val wrappedDf = df.wrap()


      val query = wrappedDf.map { f =>
        RepRow("l" -> f("language"), "users" -> f("users"))
      }.leaveNRC()

      query.show(false)
    }
    it("Simple New Style Map 2") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe2
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()


      val query = wrappedDf.map { f =>
        RepRow("n" -> f("language"), "users" -> f("users"), "c1" -> wrappedDf2.map{ o =>
        RepRow("l" -> o("lng"), "u" -> o("usr"))
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }
    it("Simple New Style Map 3") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe2
      val df3 = simpleIntDataframe3
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()


      val query = wrappedDf.map { f =>
        RepRow("language" -> f("language"), "users" -> f("users"), "c1" -> wrappedDf2.map{ o =>
        RepRow("lng" -> o("lng"), "usr" -> o("usr"), "d1" -> wrappedDf3.map{ z =>
          RepRow("l" -> z("lName"))
        })
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }
    it("Separated New Style Map 1") {
      val df = simpleIntDataframe
      val df2 = simpleIntDataframe2
      val df3 = simpleIntDataframe3
      val df5 = simpleIntDataframe5
      val wrappedDf = df.wrap()
      val wrappedDf2 = df2.wrap()
      val wrappedDf3 = df3.wrap()
      val wrappedDf5 = df5.wrap()

      val pquery = wrappedDf3.map { f =>
        RepRow(f("lName"), "nested" -> wrappedDf5.map { g =>
          RepRow(g("inUse"))
        })
      }

      val oquery = wrappedDf.map { f =>
        RepRow(f("language"), "c1" -> wrappedDf2.map{ o =>
        RepRow(o("lng"), o("usr"), "d1" -> pquery.map { d =>
          If(d("lName") === "Java") (RepRow("inner" -> d("nested"))) (RepRow.empty)
        })
      })}.leaveNRC()

      oquery.show(false)
      oquery.printSchema()
    }

    it("Nested Map Project") {
      val nestedDf = nestedDataframe
      val wrappedNestedDf = nestedDf.wrap()


      val query = wrappedNestedDf.map{ f =>
        RepRow(f)
      }

      val out = query.leaveNRC()

      out.show(false)
      out.printSchema()

    }

    it("Project Nested Struct from Nested Map") {
      val nDf1 = nestedDataframe
      val nDf2 = nestedDataframe2
      val wrappedNDf1 = nDf1.wrap()
      val wrappedNDf2 = nDf2.wrap()

      val oquery = wrappedNDf1.map{f => RepRow(f("info"))}

      val query = wrappedNDf2.map{t =>
        RepRow(t("stats"), "i" -> oquery.map{ o =>
          o("info").map{ i =>
            RepRow(i("users"))
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Plan Unnest Use Test") {
      val cop = COP
      val wrappedCOP = cop.wrap()

      val query = wrappedCOP.map { c =>
        val k = c("corders")

        val o = k.map { f => RepRow(f("dateID")) }
        o
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test Full Projection of Nested Fields") {
      val cop = COP

      val wrappedCOP = cop.wrap()

      val query = wrappedCOP.map{c =>

        c("corders").map { cr =>
          RepRow(c, cr)
        }
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Mult Projection Test") {
      val lineItem = LineItem
      val wrappedLine = lineItem.wrap()


      val query = wrappedLine.map(l => RepRow("MultCol" -> l("l_orderkey") * l("l_partkey"))).leaveNRC()

      query.show(false)
      query.printSchema()



    }

    it("Nested Math Col Test") {
      val df1 = simpleIntDataframe
      val df2 = simpleIntDataframe2

      val wrappedDf1 = df1.wrap()
      val wrappedDf2 = df2.wrap()

      val query = wrappedDf1.map{ f =>
        RepRow(f("language"), "nestedCol" -> wrappedDf2.map{z =>
          RepRow(f("users"), z("usr"), "total" -> f("users") * z("usr"))
        })
      }

      val schema = query.schema


      val output = query.leaveNRC()

      output.show(false)
      output.printSchema()

    }

    it("Nested Array Type Test") {
      val arrayCOP = ArrayCOP
      val wrappedArrayCOP = arrayCOP.wrap()

      val query = wrappedArrayCOP.map { c =>
        RepRow(c)
      }.leaveNRC()


      query.show(false)
      query.printSchema()
    }


    it("temp comp cop test") {
      val cop = COP.wrap()


      val query = cop.map { c =>
        c("corders").map(f =>
          RepRow(c, f))
      }.leaveNRC()
      query.show(false)
      query.printSchema()
    }
    it("Nested Array Type Nested Test") {
      val arrayCOP = ArrayCOP
      val wrappedArrayCOP = arrayCOP.wrap()

      val preQuery = wrappedArrayCOP.map { c =>
       RepRow("date" -> c("corders").flatMap(co => RepSeq(RepRow(co("odate"), co("dateID")))))
      }

      val schema = preQuery.schema

      val query = preQuery.leaveNRC()


      query.show(false)
      query.printSchema()
    }

    it("Nested Array TroubleShooting") {
      val arrayCOP = ArrayCOP2Arrauys.wrap()

      val preQuery = arrayCOP.map{ c =>
        RepRow(
          c("cname"),
          "dateFlat" -> c("corders2").flatMap(fc => RepSeq(RepRow(fc("odate2"), fc("dateID2")))),
        "dateArr" -> c("corders").flatMap(co => RepSeq(RepRow(co("odate"), co("dateID")))))
      }

      val query = preQuery.leaveNRC()


      query.show(false)
      query.printSchema()

    }

    it("Mixed COP Type Test") {
      val mixedCOP = MixedCOP.wrap().leaveNRC()
      mixedCOP.show(false)
      mixedCOP.printSchema()
    }
//    it("Plan Unnest Use Test - Schema Checking") {
//      val cop = COP
//      val wrappedCOP = cop.wrap()
//
//      val preQuery = wrappedCOP.map { c =>
//        val k = c("corders")
//
//        val o = k.flatMap { f =>
//          If(f("dateID") === "2") {
//            RepSeq(RepRow(f("dateID")))
//          } {
//            RepSeq.empty
//          }
//        }
//        o
//      }
//
//
//      val preQuerySchema = preQuery.schema
//
//
//      val query = preQuery.leaveNRC()
//
//      query.show(false)
//      query.printSchema()
//    }

  }

//  describe("FlatMap") {
//
//
//    //    it("Simple New Style Map") {
//    //      val df = simpleIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val query = wrappedDf.flatMap { f =>
//    //        RepRow(f("language"), f("users"))
//    //      }.as("language", "users").leaveNRC()
//    //
//    //      query.show(false)
//    //    }
//    //    it("Simple New Style Map 2 levels nest") {
//    //      val df = simpleIntDataframe
//    //      val df2 = simpleIntDataframe2
//    //      val wrappedDf = df.wrap()
//    //      val wrappedDf2 = df2.wrap()
//    //
//    //      val innerQuery = wrappedDf2.flatMap { o =>
//    //        RepRow(o("lng"), o("usr"))
//    //      }
//    //
//    //
//    //      val query = wrappedDf.flatMap { f =>
//    //        RepRow(f("language"), innerQuery.flatMap{ o =>
//    //          RepRow(o("lng"))
//    //        })
//    //      }.leaveNRC()
//    //
//    //      query.show(false)
//    //    }
//
//    it("Simple New Style Map 3 levels nest") {
//      val df = simpleIntDataframe
//      val df2 = simpleIntDataframe2
//      val df3 = simpleIntDataframe3
//
//      val wrappedDf = df.wrap()
//      val wrappedDf2 = df2.wrap()
//      val wrappedDf3 = df3.wrap()
//
//      val innerQuery = wrappedDf2.flatMap { o =>
//        RepRow(o("usr"))
//      }.as("usr")
//
//
//      val query = wrappedDf.flatMap { f =>
//        RepRow(f("language"), RepProjection("inner", innerQuery))
//      }.leaveNRC()
//
//      query.show(false)
//    }
//
//
//        it("Successful FlatMap - Integer Column Addition, Sequence of Rows Output") {
//          val df = pureIntDataframe
//          val wrappedDf = df.wrap()
//
//          val expected = df.flatMap { x =>
//            val id = x.getInt(0)
//            val users = x.getInt(1)
//            Seq(
//              SparkRow(id, users),
//              SparkRow(id + 10, users + 10)
//            )
//          }(RowEncoder(df.schema))
//          expected.show()
//
//          val res = wrappedDf.flatMap { x =>
//            val id = x.get(0)
//            val users = x.get(1)
//            RepSeq(
//              RepRow(id, users),
//              RepRow(id + 10, users + 10)
//            )
//          }.leaveNRC()
//          res.show()
//
//          assertDataFramesAreEquivalent(res, expected)
//
//        }
//    //   it("Successful FlatMap - Integer Column Addition, Sequence of 3 Rows Output") {
//    //        val df = pureIntDataframe
//    //        val wrappedDf = df.wrap()
//    //
//    //        val expected = df.flatMap { x =>
//    //          val id = x.getInt(0)
//    //          val users = x.getInt(1)
//    //          Seq(
//    //            SparkRow(id, users),
//    //            SparkRow(id + 10, users + 10),
//    //            SparkRow(id + 20, users + 20)
//    //          )
//    //        }(RowEncoder(df.schema))
//    //        expected.show()
//    //
//    //        val res = wrappedDf.flatMap { x =>
//    //          val id = x.get(0)
//    //          val users = x.get(1)
//    //          RepSeq(
//    //            RepRow(id, users),
//    //            RepRow(id + 10, users + 10),
//    //            RepRow(id + 20, users + 20)
//    //          )
//    //        }.leaveNRC()
//    //        res.show()
//    //
//    //        assertDataFramesAreEquivalent(res, expected)
//    //
//    //      }
//    //
//    //    it("Flat to Nested With Dummy Data") {
//    //      val df = simpleIntDataframe3
//    //      val df2 = simpleIntDataframe5
//    //
//    //
//    //      val joinedDf = df.join(df2, df("lName") === df2("language"), "left_outer")
//    //      val colum = functions.struct(joinedDf("users"), joinedDf("inUse"))
//    //      val dfWithColumn = joinedDf.withColumn("nested", colum)
//    //      val sparkResWithColumn = dfWithColumn.select("lName", "nested")
//    //
//    //      sparkResWithColumn.show()
//    //
//    //
//    //      val wrappedDf = df.wrap()
//    //      val wrappedDf2 = df2.wrap()
//    //
//    //      val res = wrappedDf.flatMap{f =>
//    //          val lName = f.get(0)
//    //          RepRow(lName, wrappedDf2.flatMap{ z =>
//    //            val language = z.get(0)
//    //            val users = z.get(1)
//    //            val inUse = z.get(2)
//    //            repIf(lName === language) {
//    //              RepRow(users, inUse)
//    //            } {
//    //              RepRow()
//    //            }
//    //          })
//    //      }.leaveNRC()
//    //
//    //      res.show()
//    //
//    //
//    //      assertDataFramesAreEquivalent(sparkResWithColumn, res)
//    //    }
//    //
//    //    it("Simpler Nested FlatMap Attempt") {
//    //      val df = simpleIntDataframe
//    //      val df2 = simpleIntDataframe3
//    //      val df3 = simpleIntDataframe2
//    //
//    //      val wrappedDf = df.wrap()
//    //      val wrappedDf2 = df2.wrap()
//    //      val wrappedDf3 = df3.wrap()
//    //
//    //       val query2_1 = wrappedDf2.flatMap{f =>
//    //        RepRow(f("lName"), f("userNo"))
//    //        }
//    //
//    //      val query2_2 = wrappedDf.flatMap{f =>
//    //        RepRow(f.get(0), query2_1.flatMap{o =>
//    //          repIf(f("language") === o("lName")) {
//    //            RepRow(o("lName"))
//    //          } {
//    //            RepRow.empty
//    //          }
//    //        })
//    //      }.leaveNRC()
//    //
//    //      query2_2.show(false)
//    //      query2_2.printSchema()
//    //    }
//    //
//    it("Simpler Nested FlatMap Attempt - nested struct separated query") {
//      val df = simpleIntDataframe
//      val df2 = simpleIntDataframe3
//      val df3 = simpleIntDataframe2
//
//      val wrappedDf = df.wrap()
//      val wrappedDf2 = df2.wrap()
//      val wrappedDf3 = df3.wrap()
//
//      wrappedDf2.groupBy("").sum("")
//
//
//
//      val schemaTest1 = wrappedDf.schema
//      val query = wrappedDf2.map { f =>
//          RepRow("n" -> f("lName"), "collectionU" -> f("userNo"))
//      }
//
//      query.flatMap { f =>
//        val o = f("collectionU").asCollection()
//
//
//      }
//      val schemaTest3 = wrappedDf2.flatMap { f =>
//        RepSeq(
//          RepRow("n" -> f("lName"))
//        )
//      }.schema
//
//      val query2_1 = wrappedDf2.flatMap { f =>
//        RepRow("language" -> f("language"), "output" -> wrappedDf3.flatMap { o =>
//          RepRow("d1" -> o("lng"))
//        })
//      }
////
////      val firstPartSchema = query2_1.schema
////
////      val query2_2 = wrappedDf.flatMap { f =>
////        RepRow(query2_1.flatMap { o =>
////          RepRow(o("d1"))
////        }.as("b1"))
////      }.leaveNRC()
////
////      query2_2.show(false)
////      query2_2.printSchema()
//    }
//    //
//        it ("Simpler Nested FlatMap Attempt - nested struct single query") {
//          val df = simpleIntDataframe
//          val df2 = simpleIntDataframe3
//          val df3 = simpleIntDataframe2
//
//          val wrappedDf = df.wrap()
//          val wrappedDf2 = df2.wrap()
//          val wrappedDf3 = df3.wrap()
//
//          val query2_2 = wrappedDf.flatMap { f =>
//            RepRow(wrappedDf2.flatMap { o =>
//              repIf(f("language") === o("lName")) {
//                RepRow(o("lName"), wrappedDf3.flatMap {z =>
//                  RepRow(z("lng"), z("usr"))
//                }.as("c1"))
//              } {
//                RepRow.empty
//              }
//            }.as("b1"))
//          }.leaveNRC()
//
//
//
//
//          query2_2.show(false)
//          query2_2.printSchema()
//
//        }
//
//
//    //    it("New Sym Test") {
//    //      val df = simpleIntDataframe
//    //      val df2 = simpleIntDataframe2
//    //
//    //      val wrappedDf = df.wrap()
//    //      val wrappedDf2 = df2.wrap()
//    //
//    //      val query = wrappedDf.flatMap{f =>
//    //        val language = f("language")
//    //        RepRow(language, wrappedDf2.flatMap{ o =>
//    //          val lng = o("lng")
//    //          val usr = o("usr")
//    //          val k = RepRow(lng, usr)
//    //          val k2 = k("lng")
//    //          RepRow(k2)
//    //        })
//    //      }.leaveNRC()
//    //
//    //      query.show(false)
//    //    }
//
//  }
//  //TODO divide type promotion works differently in map
//  describe("Map") {
//    it("Successful Map - Identity Function") {
//      val df = pureIntDataframe
//      val wrappedDf = df.wrap()
//
//      val expected = df.map(x => x)(RowEncoder(df.schema))
//      expected.show()
//
//      val res = wrappedDf.map(x => x).leaveNRC()
//      res.show()
//
//      assertDataFramesAreEquivalent(res, expected)
//    }
//
//    it("Successful Map - Identity Function with Renamed columns") {
//      val df = pureIntDataframe
//      val wrappedDf = df.wrap()
//
//
//      val schema: StructType = StructType(Seq(
//        StructField("language", IntegerType, nullable = true),
//        StructField("info", IntegerType, nullable = true
//        )))
//
//
//      val expected = df.map(x => x)(RowEncoder(schema))
//      expected.show()
//
//      val res = wrappedDf.map(x => x).leaveNRC()
//      res.show()
//
//      assertDataFramesAreEquivalent(res, expected)
//      assertDataFrameSchemaEqual(res, expected)
//    }
//
//    //    it("Successful Map - Integer Column + 1") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] + 1)
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //
//    //        val k = x.toSeq.map(f => f + 1)
//    //
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //
//    //
//    //    it("Successful Map - Integer Column Multiples Additions") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] + 1 + 2)
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f + 1 + 2)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //    it("Successful Map - Integer Column Added to Itself") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int])
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f + f)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //    it("Successful Map - Integer Column Added to Itself Twice") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int] + f.asInstanceOf[Int])
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f + f + f)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //
//    //    it("Successful Map - Integer Column Added to Itself Multiple Times") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] + f.asInstanceOf[Int] + f.asInstanceOf[Int] + 2 + f.asInstanceOf[Int] + 3 + f.asInstanceOf[Int])
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f + f + f + 2 + f + 3 + f)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //
//    //    it("Successful Map - Integer Mod with brackets") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] % (f.asInstanceOf[Int] + 1))
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f % (f + 1))
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //
//    //    it("Successful Map - Integer Column Minus to Itself") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] - f.asInstanceOf[Int] - f.asInstanceOf[Int])
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f - f - f)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //
//    //    // TODO - type promotion for double shouldn't happen here
//    //    it("Successful Map - Integer All Operations Tuple Both Sides Issue") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val expected = df.map { x =>
//    //        val k = x.toSeq.map(f => f.asInstanceOf[Int] - f.asInstanceOf[Int] + f.asInstanceOf[Int] * f.asInstanceOf[Int] / f.asInstanceOf[Int] % f.asInstanceOf[Int])
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //      expected.show()
//    //
//    //      val res = wrappedDf.map { x =>
//    //        val k = x.toSeq.map(f => f - f + f * f / f % f)
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //      res.show()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //    }
//    //    // TODO - Use UDF?
//    //    // TODO Reimpliment after changes to RepRow
//    ////    it("Successful Map - String Column Modification") {
//    ////      val df = pureStringDataframe
//    ////      val wrappedDf = df.wrap()
//    ////
//    ////      val expected = df.map { x =>
//    ////        SparkRow.fromSeq(x.toSeq.map(f => s"${f.asInstanceOf[String]}: Test"))
//    ////      }(RowEncoder(df.schema))
//    ////      expected.show()
//    ////
//    ////      val res = wrappedDf.map { x =>
//    ////        val k = x.toSeq.map { f =>
//    ////          val k = f + ": Test"
//    ////          k
//    ////        }
//    ////        RepRow.fromSeq(k)
//    ////      }.leaveNRC()
//    ////      res.show(false)
//    ////
//    ////      assertDataFramesAreEquivalent(res, expected)
//    ////    }
//    //    //
//    //    //it("Successful Map - String Column Modification Alternate Way") {
//    //    //      val df = pureStringDataframe
//    //    //      val wrappedDf = df.wrap()
//    //    //
//    //    //      val expected = df.map { x =>
//    //    //        SparkRow.fromSeq(x.toSeq.map(f => "Replaced"))
//    //    //      }(RowEncoder(df.schema))
//    //    //      expected.show()
//    //    //
//    //    //      import uk.ac.ox.cs.trance.repextensions._
//    //    //      val res = wrappedDf.map{ x =>
//    //    //       val k = x.toSeq.tMap(x => "Replaced")
//    //    //        RepRow.fromSeq(k)
//    //    //      }(RepRowEncoder(df.schema)).leaveNRC()
//    //    //      res.show(false)
//    //    //
//    //    //      assertDataFramesAreEquivalent(res, expected)
//    //    //    }
//    //
//    //    it("moreTesting - conditional if in integer addition mapping") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val out = df.map { x =>
//    //        val k = x.toSeq.map { f =>
//    //          if (f.asInstanceOf[Int] > 50) {
//    //            f.asInstanceOf[Int] + 29
//    //          } else {
//    //            f
//    //          }
//    //        }
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(df.schema))
//    //
//    //      out.show()
//    //
//    //
//    //      val res = wrappedDf.map { x =>
//    //        println("RepRow: " + x)
//    //
//    //        val k = x.toSeq.map { f =>
//    //          repIf(f > 50) {
//    //            f + 29
//    //          } {
//    //            f
//    //          }
//    //        }
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //
//    //      res.show()
//    //      assertDataFramesAreEquivalent(res, out)
//    //      assertDataFrameSchemaEqual(res, out)
//    //
//    //
//    //    }
//    //
//    //    it("Map change Integer output columns in RowEncoder") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val schema: StructType = StructType(Seq(
//    //        StructField("language", IntegerType, nullable = true),
//    //        StructField("info", IntegerType, nullable = true
//    //        )))
//    //
//    //      val expected = df.map { f =>
//    //        val k = Seq(f.getInt(1), f.getInt(0))
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(schema))
//    //
//    //      val res = wrappedDf.map { f =>
//    //        val k = Seq(f.get(1), f.get(0))
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //
//    //      expected.show()
//    //      res.show()
//    //      assertDataFramesAreEquivalent(res, expected)
//    //      assertDataFrameSchemaEqual(res, expected)
//    //
//    //
//    //    }
//    //    it("Map change String output columns in RowEncoder") {
//    //      val df = pureStringDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val schema: StructType = StructType(Seq(
//    //        StructField("language", StringType, nullable = true),
//    //        StructField("info", StringType, nullable = true
//    //        )))
//    //
//    //      val expected = df.map { f =>
//    //        val k = Seq(f.getString(1), f.getString(0))
//    //        SparkRow.fromSeq(k)
//    //      }(RowEncoder(schema))
//    //
//    //      val res = wrappedDf.map { f =>
//    //        val k = Seq(f.get(1), f.get(0))
//    //        RepRow.fromSeq(k)
//    //      }.leaveNRC()
//    //
//    //      expected.show()
//    //      res.show()
//    //      assertDataFramesAreEquivalent(res, expected)
//    //      assertDataFrameSchemaEqual(res, expected)
//    //
//    //
//    //    }
//    it("Change Same Number of Column Names in Map") {
//      val df = pureIntDataframe
//      val wrappedDf = df.wrap()
//
//      val schema: StructType = StructType(Seq(
//        StructField("column1", IntegerType, nullable = true),
//        StructField("column2", IntegerType, nullable = true
//        )))
//
//
//      val expected = df.map { f => SparkRow(f.get(0), f.get(1)) }(RowEncoder(schema))
//
//      expected.show()
//      expected.printSchema()
//
//      val res = wrappedDf.map { f => RepRow(f.get(0), f.get(1)) }.leaveNRC()
//
//      res.show()
//      res.printSchema()
//
//      assertDataFramesAreEquivalent(res, expected)
//      assertDataFrameSchemaEqual(res, expected)
//    }
//
//    //    it("Map change Int to Double with Arithmetic Expression - Same Column Names") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val schema: StructType = StructType(Seq(
//    //        StructField("column1", DoubleType, nullable = true),
//    //        StructField("column2", DoubleType, nullable = true
//    //        )))
//    //
//    //      val expected = df.map { f =>
//    //        val column1 = f.getInt(0) * 2.5
//    //        val column2 = f.getInt(1) * 1.5
//    //        SparkRow(column1, column2)
//    //      }(RowEncoder(schema))
//    //
//    //      expected.show()
//    //      expected.printSchema()
//    //
//    //      val res = wrappedDf.map { f =>
//    //        val column1 = f.get(0) * 2.5
//    //        val column2 = f.get(1) * 1.5
//    //        RepRow(column1, column2)
//    //      }.leaveNRC()
//    //
//    //      res.show()
//    //      res.printSchema()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //      assertDataFrameSchemaEqual(res, expected)
//    //    }
//    //TODO reimplement string transformation
//    //    it("Map change Int output types to String in RowEncoder different column names") {
//    //      val df = pureIntDataframe
//    //      val wrappedDf = df.wrap()
//    //
//    //      val schema: StructType = StructType(Seq(
//    //        StructField("helloColumn", StringType, nullable = true),
//    //        StructField("users", DoubleType, nullable = true
//    //        )))
//    //
//    //      val expected = df.map { f =>
//    //        val column1 = "Hello"
//    //        val column2 = f.getInt(1) * 1.5
//    //        SparkRow(column1, column2)
//    //      }(RowEncoder(schema))
//    //
//    //      expected.show()
//    //      expected.printSchema()
//    //
//    //      val res = wrappedDf.map { f =>
//    //        val column1 = Alias(f.get(0), "Hello")
//    //        val column2 = f.get(1) * 1.5
//    //        RepRow(column1, column2)
//    //      }(RepRowEncoder(schema)).leaveNRC()
//    //
//    //      res.show()
//    //      res.printSchema()
//    //
//    //      assertDataFramesAreEquivalent(res, expected)
//    //      assertDataFrameSchemaEqual(res, expected)
//    //    }
//  }

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

    // WIP Select
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
//  // TODO - WIP
  describe("tpch tests") {
//        it("Flat to Nested With Dummy Data") {
//          val df = simpleIntDataframe3
//          val df2 = simpleIntDataframe5
//
//
//          val joinedDf = df.join(df2, df("lName") === df2("language"), "left_outer")
//          val colum = functions.struct(joinedDf("users"), joinedDf("inUse"))
//          val dfWithColumn = joinedDf.withColumn("nested", colum)
//          val sparkResWithColumn = dfWithColumn.select("lName", "nested")
//
//          sparkResWithColumn.show()
//
//
//          val wrappedDf = df.wrap()
//          val wrappedDf2 = df2.wrap()
//
//          val res = wrappedDf.flatMap { f =>
//            val lName = f.get(0)
//            RepRow(lName, wrappedDf2.flatMap { z =>
//              val language = z.get(0)
//              val users = z.get(1)
//              val inUse = z.get(2)
//              repIf(lName === language) {
//                RepRow(users, inUse)
//              } {
//                RepRow()
//              }
//            })
//          }.leaveNRC()
//
//          res.show()
//
//
//          assertDataFramesAreEquivalent(sparkResWithColumn, res)
//        }
//
//
//        it("FlatToNested - Test1") {
//          val lineItem = LineItem
//
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val oquery = wrappedOrder.flatMap { f =>
//            val o_orderdate = f.get(4)
//            val o_orderkey = f.get(0)
//            RepRow(o_orderdate, wrappedLineItem.flatMap { z =>
//              val l_orderkey = z.get(0)
//              val l_partkey = z.get(1)
//              val l_quantity = z.get(4)
//              repIf(l_orderkey === o_orderkey) {
//                RepRow(l_partkey, l_quantity)
//              } {
//                RepRow.empty
//              }
//            }.as("o_parts"))
//          }.leaveNRC()
//
//          oquery.show()
//
//        }
//
//        it("FlatToNested Test1Full") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//
//          val oquery = wrappedOrder.flatMap { f =>
//            val o_orderkey = f.get(0)
//            RepRow(f, wrappedLineItem.flatMap { z =>
//              val l_orderkey = z.get(0)
//              repIf(l_orderkey === o_orderkey) {
//                RepRow(z)
//              } {
//                RepRow.empty
//              }
//            })
//          }.leaveNRC()
//
//          oquery.show(false)
//
//        }
//        it("FlatToNested Test2") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val customer = Customer
//          val wrappedCustomer = customer.wrap()
//
//
//          val query = wrappedCustomer.flatMap { c =>
//            val c_custkey = c.get(0)
//            val c_name = c.get(1)
//            RepRow(c_name, wrappedOrder.flatMap { o =>
//              val o_orderkey = o.get(0)
//              val o_custkey = o.get(1)
//              val o_orderdate = o.get(4)
//              repIf(c_custkey === o_custkey) {
//                RepRow(o_orderdate, wrappedLineItem.flatMap { l =>
//                  val l_orderkey = l.get(0)
//                  val l_partkey = l.get(1)
//                  val l_quantity = l.get(4)
//                  repIf(l_orderkey === o_orderkey) {
//                    RepRow(l_partkey, l_quantity)
//                  } {
//                    RepRow.empty
//                  }
//                }.as("o_parts"))
//              } {
//                RepRow.empty
//              }
//            }.as("c_orders"))
//          }.leaveNRC()
//
//          query.show(false)
//          query.printSchema()
//
//        }
//      }
//
//    it("FlatToNested Test2Filter") {
//      val lineItem = LineItem
//      val wrappedLineItem = lineItem.wrap()
//
//      val order = Order
//      val wrappedOrder = order.wrap()
//
//      val customer = Customer
//      val wrappedCustomer = customer.wrap()
//
//      val query = wrappedCustomer.flatMap { c =>
//        val c_custkey = c.get(0)
//        val c_name = c.get(1)
//        val c_nationkey = c.get(3)
//        RepRow(c_name, wrappedOrder.flatMap { o =>
//          val o_orderkey = o.get(0)
//          val o_custkey = o.get(1)
//          val o_orderdate = o.get(4)
//          repIf(c_custkey === o_custkey && c_nationkey === 1) {
//              RepRow(o_orderdate, wrappedLineItem.flatMap { l =>
//                val l_orderkey = l.get(0)
//                val l_partkey = l.get(1)
//                val l_quantity = l.get(4)
//                repIf(l_orderkey === o_orderkey) {
//                  RepRow(l_partkey, l_quantity)
//                } {
//                  RepRow.empty
//                }
//              }.as("o_parts"))
//            } {
//              RepRow.empty
//            }
//        }.as("c_orders"))
//      }.leaveNRC()
//
//      query.show(false)
//      query.printSchema()
//
//
//    }
//
//        it("FlatToNested Test2Flat - Working Example Top Level Select") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val customer = Customer
//          val wrappedCustomer = customer.wrap()
//
//          val oquery = wrappedOrder.flatMap { o =>
//            val o_orderkey = o.get(0)
//            val o_custkey = o.get(1)
//            val o_orderdate = o.get(4)
//            RepRow(o_custkey, o_orderdate, wrappedLineItem.flatMap { l =>
//              val l_orderkey = l.get(0)
//              val l_partkey = l.get(1)
//              val l_quantity = l.get(4)
//              repIf(l_orderkey === o_orderkey) {
//                RepRow(l_partkey, l_quantity)
//              } {
//                RepRow.empty
//              }
//            })
//          }
//
//          val query = wrappedCustomer.flatMap { c =>
//            val c_custkey = c.get(0)
//            val c_name = c.get(1)
//            RepRow(c_name, oquery.flatMap { o =>
//              val o_custkey = o.get(1)
//              repIf(o_custkey === c_custkey) {
//                RepRow(o("o_custkey"), o("o_orderdate"))
//              } {
//                RepRow.empty
//              }
//            }.as("c_orders"))
//          }.leaveNRC()
//
//
//          query.show(false)
//          query.printSchema()
//
//
//        }
//
//        it("FlatToNested Test2Flat - Working Without splitting up query") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val customer = Customer
//          val wrappedCustomer = customer.wrap()
//
//
//          val cquery = wrappedCustomer.flatMap { c =>
//            val c_name = c("c_name")
//            val c_custkey = c("c_custkey")
//            RepRow(c_name, wrappedOrder.flatMap { o =>
//              val o_orderdate = o("o_orderdate")
//              val o_orderkey = o("o_orderkey")
//              val o_custkey = o("o_custkey")
//              repIf(o_custkey === c_custkey) {
//                RepRow(o_orderdate, wrappedLineItem.flatMap { l =>
//                  val l_orderkey = l("l_orderkey")
//                  val l_partkey = l("l_partkey")
//                  val l_quantity = l("l_quantity")
//                  repIf(o_orderkey === l_orderkey) {
//                    RepRow(l_partkey, l_quantity)
//                  } {
//                    RepRow.empty
//                  }
//                }.as("o_parts"))
//              } {
//                RepRow.empty
//              }
//            }.as("c_orders"))
//          }.leaveNRC()
//
//          cquery.show(false)
//          cquery.printSchema()
//        }
//        it("FlatToNested Test2Flat - Error Example Top Level Select of Nested Field") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val customer = Customer
//          val wrappedCustomer = customer.wrap()
//
//          val oquery = wrappedOrder.flatMap { o =>
//            val o_orderkey = o.get(0)
//            val o_custkey = o.get(1)
//            val o_orderdate = o.get(4)
//            RepRow(o_custkey, o_orderdate, wrappedLineItem.flatMap { l =>
//              val l_orderkey = l.get(0)
//              val l_partkey = l.get(1)
//              val l_quantity = l.get(4)
//              repIf(l_orderkey === o_orderkey) {
//                RepRow(l_partkey, l_quantity)
//              } {
//                RepRow.empty
//              }
//            }.as("o_parts"))
//          }
//
//          val query = wrappedCustomer.flatMap { c =>
//            val c_custkey = c.get(0)
//            val c_name = c.get(1)
//            RepRow(c_name, oquery.flatMap { o =>
//              val o_custkey = o.get(1)
//              repIf(o_custkey === c_custkey) {
//                RepRow(o("o_custkey"), o("o_parts"))
//              } {
//                RepRow.empty
//              }
//            }.as("c_orders"))
//          }.leaveNRC()
//
//
//          query.show(false)
//          query.printSchema()
//
//
//        }
//        it("FlatToNested Test2Flat - Error Example Top Level o_parts select") {
//          val lineItem = LineItem
//          val wrappedLineItem = lineItem.wrap()
//
//          val order = Order
//          val wrappedOrder = order.wrap()
//
//          val customer = Customer
//          val wrappedCustomer = customer.wrap()
//
//          val oquery = wrappedOrder.flatMap { o => // Sym(WrappedArray(RepElem(o_orderkey,s4), RepElem(o_custkey,s4), RepElem(o_orderstatus,s4), RepElem(o_totalprice,s4), RepElem(o_orderdate,s4), RepElem(o_orderpriority,s4), RepElem(o_clerk,s4), RepElem(o_shippriority,s4), RepElem(o_comment,s4)))
//            val o_orderkey = o.get(0)
//            val o_custkey = o.get(1)
//            val o_orderdate = o.get(4)
//            RepRow(o_custkey, o_orderdate, wrappedLineItem.flatMap { l => // Sym(WrappedArray(RepElem(l_orderkey,s5), RepElem(l_partkey,s5), RepElem(l_suppkey,s5), RepElem(l_linenumber,s5), RepElem(l_quantity,s5), RepElem(l_extendedprice,s5), RepElem(l_discount,s5), RepElem(l_tax,s5), RepElem(l_returnflag,s5), RepElem(l_linestatus,s5), RepElem(l_shipdate,s5), RepElem(l_commitdate,s5), RepElem(l_receiptdate,s5), RepElem(l_shipinstruct,s5), RepElem(l_shipmode,s5), RepElem(l_comment,s5), RepElem(uniqueId,s5)))
//              val l_orderkey = l.get(0)
//              val l_partkey = l.get(1)
//              val l_quantity = l.get(4)
//              repIf(l_orderkey === o_orderkey) {
//                RepRow(l_partkey, l_quantity)
//              } {
//                RepRow.empty
//              }
//            }.as("o_parts"))
//          }
//
//          //Query without flatMap on flatMap
//          //    val query = wrappedCustomer.flatMap{ c =>
//          //      val c_custkey = c.get(0)
//          //      val c_name = c.get(1)
//          //      RepRow(c_name, oquery)
//          //    }.leaveNRC()
//
//          val query = wrappedCustomer.flatMap { c => // Sym(WrappedArray(RepElem(c_custkey,s6), RepElem(c_name,s6), RepElem(c_address,s6), RepElem(c_nationkey,s6), RepElem(c_phone,s6), RepElem(c_acctbal,s6), RepElem(c_mktsegment,s6), RepElem(c_comment,s6)))
//            val c_custkey = c.get(0)
//            val c_name = c.get(1)
//            RepRow(c_name, oquery.flatMap { o => // Sym(List(RepElem(o_orderkey,s4), RepElem(o_custkey,s4), RepElem(o_orderstatus,s4), RepElem(o_totalprice,s4), RepElem(o_orderdate,s4), RepElem(o_orderpriority,s4), RepElem(o_clerk,s4), RepElem(o_shippriority,s4), RepElem(o_comment,s4), RepElem(o_custkey,s4), RepElem(o_orderdate,s4), RepElem(l_orderkey,s5), RepElem(l_partkey,s5), RepElem(l_suppkey,s5), RepElem(l_linenumber,s5), RepElem(l_quantity,s5), RepElem(l_extendedprice,s5), RepElem(l_discount,s5), RepElem(l_tax,s5), RepElem(l_returnflag,s5), RepElem(l_linestatus,s5), RepElem(l_shipdate,s5), RepElem(l_commitdate,s5), RepElem(l_receiptdate,s5), RepElem(l_shipinstruct,s5), RepElem(l_shipmode,s5), RepElem(l_comment,s5), RepElem(uniqueId,s5), RepElem(l_orderkey,s5), RepElem(o_orderkey,s4), RepElem(l_partkey,s5), RepElem(l_quantity,s5)))
//              val o_custkey = o("o_custkey")
//              repIf(o_custkey === c_custkey) {
//                RepRow(o("o_orderdate"), o("o_parts"))
//              } {
//                RepRow.empty
//              }
//            })
//          }.leaveNRC()
//
//
//          query.show(false)
//          query.printSchema()
//
//
//        }
//
////        it("Paper Test") {
////          val cop = COP
////          val part = PART
////
////          val wrappedCop = cop.wrap()
////          val wrappedPart = part.wrap()
////
////          val query = wrappedCop.flatMap{f =>
////            val cname = f("cname")
////            val corders = f("corders")
////            RepRow(cname, f("corders").flatMap{c =>
////              val odate = c("odate")
////              RepRow(odate, )
////            }.as("o_parts"))
////          }
////        }
//
//
//        it("Generated Code Test") {
//                import spark.implicits._
//
//                val Lineitem = LineItem
//                Lineitem.cache
//                Lineitem.count
//
//                val Customer = TestDataframes.Customer
//                Customer.cache
//                Customer.count
//
//                val x15 = Customer.select("c_name")
//
//                  .as[Record81f38c97198a4c3d82c07bdf8cc21c89]
//
//                val x16 = x15.withColumn("Customer_index", monotonically_increasing_id())
//                  .as[Recorddfd1279ee2b64495a6067f8c39892143]
//
//                val x18 = Lineitem.select("l_orderkey")
//
//                  .as[Record9e5847f902704689a1c0cbd58204e07c]
//
//                val x21 = x16.crossJoin(x18)
//                  .as[Recordb8662d38214c4eccbca05aff64ad7253]
//
//                val x23 = x21
//
//
//                val x25 = x23.groupByKey(x24 => Recorddfd1279ee2b64495a6067f8c39892143(x24.c_name, x24.Customer_index)).mapGroups {
//                  case (key, value) =>
//                    val grp = value.flatMap(x24 =>
//                      (x24.l_orderkey) match {
//                        case (None) => Seq()
//                        case _ => Seq(Record9e5847f902704689a1c0cbd58204e07c(x24.l_orderkey match { case Some(x) => x; case _ => 0 }))
//                      }).toSeq
//                    Recordf844ee1edbc947c29fc12a7a1429ced2(key.c_name, grp)
//                }.as[Recordf844ee1edbc947c29fc12a7a1429ced2]
//
//                val x26 = x25
//                val TestX = x26
//                TestX.show()
//                TestX.printSchema()
//                //TestX.count
//
//              }
//
//          it("Flat to Nested Generated Test 1") {
//
//            val Lineitem = LineItem
//            Lineitem.cache
//            Lineitem.count
//            val order = Order
//            order.cache
//            order.count
//
//            import spark.implicits._
//
//
//            val x13 = order.select("o_orderdate", "o_orderkey")
//              .as[Record95de9901799643d99af0b2fc86f09fdf]
//
//            val x14 = x13.withColumn("Order_index", monotonically_increasing_id())
//              .as[Recordbb4c9b827da843bab38ab4b0cff71ad6]
//
//            val x16 = Lineitem.select("l_quantity", "l_partkey", "l_orderkey")
//
//              .as[Recordcd5962544f3d4361a05c67f62a9ec845]
//
//
//            val x19 = x14.equiJoin(x16, Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recorddd2ed07dab354fba9c7ce4139a96cc43]
//
//            val x21 = x19.select("Order_index", "o_orderdate", "l_partkey", "l_quantity")
//
//              .as[Record95020c82d35644059c5c5b406b79f713]
//
//            val x23 = x21.groupByKey(x22 => Record4948c766a27942df940a6f7f71f34218(x22.o_orderdate, x22.Order_index)).mapGroups {
//              case (key, value) =>
//                val grp = value.flatMap(x22 =>
//                  (x22.l_partkey, x22.l_quantity) match {
//                    case (None, _) => Seq()
//                    case (_, None) => Seq()
//                    case _ => Seq(Record6444f3ba076c4c409686bf6410417a86(x22.l_partkey match { case Some(x) => x; case _ => 0 }, x22.l_quantity match { case Some(x) => x; case _ => 0.0 }))
//                  }).toSeq
//                Record10598666558d460ab7cf36a3624f7fe2(key.Order_index, key.o_orderdate, grp)
//            }.as[Record10598666558d460ab7cf36a3624f7fe2]
//
//            val x24 = x23
//
//            x24.show(false)
//            x24.printSchema()
//          }

//          it("Generated Test2Filter") {
//
//            import spark.implicits._
//
//            val x20 = Customer.select("c_name", "c_nationkey", "c_custkey")
//
//              .as[Record2dc37b6c982f4fc5864bfea8830b976e]
//
//            val x21 = x20.withColumn("Customer_index", monotonically_increasing_id())
//              .as[Record99b6f24fbc634ee9b3639e57d1bf20c1]
//
//            val x23 = Order.select("o_orderdate", "o_custkey", "o_orderkey")
//
//              .as[Recorde3649c70440340cfad4b3323d08e2e3b]
//
//            val x24 = x23.withColumn("Order_index", monotonically_increasing_id())
//              .as[Record6687cebc34a748a8bb09d5668686082e]
//
//            val x27 = x21.join(x24, col("c_custkey") === col("o_custkey") && col("c_nationkey") === 1, "left_outer")
//              .as[Recordd220fd669d7e41f1b86a3aabecac4237]
//
//            val x29 = LineItem.select("l_quantity", "l_partkey", "l_orderkey")
//
//              .as[Record478e4064b2aa4a81882aa61634d044c9]
//
//            val x32 = x27.equiJoin(x29,
//              Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Record811a07786a1742d68b5b3e724eb7e675]
//
//            val x34 = x32.select("o_orderdate", "l_quantity", "Order_index", "c_name", "Customer_index", "l_partkey")
//
//              .as[Record7b14754fea1d449e84f101370561bb95]
//
//            val x36 = x34.groupByKey(x35 => Record673d683609d24f698c9fab8a214ccb52(x35.o_orderdate, x35.Order_index, x35.c_name, x35.Customer_index)).mapGroups {
//              case (key, value) =>
//                val grp = value.flatMap(x35 =>
//                  (x35.l_quantity, x35.l_partkey) match {
//                    case (None, _) => Seq()
//                    case (_, None) => Seq()
//                    case _ => Seq(Recorda14eb38fa0b14107a1e4e87ea2d35fde(x35.l_partkey match { case Some(x) => x; case _ => 0 }, x35.l_quantity match { case Some(x) => x; case _ => 0.0 }))
//                  }).toSeq
//                Recordbcc1e90432f548a2b9eb13a23cafb152(key.o_orderdate, key.Order_index, key.c_name, grp, key.Customer_index)
//            }.as[Recordbcc1e90432f548a2b9eb13a23cafb152]
//
//            val x38 = x36.select("Customer_index", "c_name", "o_orderdate", "o_parts")
//
//              .as[Record190f73c1f64d49a297297765c57c8bde]
//
//            val x40 = x38.groupByKey(x39 => Record5c1622d89e264a809d3f13e0d4a12ed8(x39.c_name, x39.Customer_index)).mapGroups {
//              case (key, value) =>
//                val grp = value.flatMap(x39 =>
//                  (x39.o_orderdate) match {
//                    case (None) => Seq()
//                    case _ => Seq(Record77304327a217441fa8f29bdd4a041d34(x39.o_orderdate match { case Some(x) => x; case _ => "null" }, x39.o_parts))
//                  }).toSeq
//                Record3df1924806c44fcdafc4395b39bd3b4c(key.Customer_index, key.c_name, grp)
//            }.as[Record3df1924806c44fcdafc4395b39bd3b4c]
//
//            val x41 = x40
//            val Test2Filter = x41
//
//            Test2Filter.show(false)
//            Test2Filter.printSchema()
//          }
//
//        it("Generated Test2Flat") {
//
//          import spark.implicits._
//          val x24 = Order.select("o_orderdate", "o_custkey", "o_orderkey")
//
//            .as[Recordb80e3302266a42e4a981836379570249]
//
//          val x25 = x24.withColumn("Order_index", monotonically_increasing_id())
//            .as[Recordd16d0fb04d69452d83bec9ee88449806]
//
//          val x27 = LineItem.select("l_quantity", "l_partkey", "l_orderkey")
//
//            .as[Record80e931fa827f40cca9ea94aa8b0a308a]
//
//          val x30 = x25.equiJoin(x27,
//            Seq("o_orderkey"), Seq("l_orderkey"), "left_outer").as[Recordbf846df439974b07b6abb5120e933eb8]
//
//          val x32 = x30.select("o_orderdate", "o_custkey", "l_quantity", "Order_index", "l_partkey")
//
//            .as[Recordd74a63caf68e4419868a969376dbf27b]
//
//          val x34 = x32.groupByKey(x33 => Recorde64677bfead1481e9fe09c808070d452(x33.o_orderdate, x33.o_custkey, x33.Order_index)).mapGroups {
//            case (key, value) =>
//              val grp = value.flatMap(x33 =>
//                (x33.l_quantity, x33.l_partkey) match {
//                  case (None, _) => Seq()
//                  case (_, None) => Seq()
//                  case _ => Seq(Record0f24c31b203c447584d817d42bc7dd63(x33.l_partkey match { case Some(x) => x; case _ => 0 }, x33.l_quantity match { case Some(x) => x; case _ => 0.0 }))
//                }).toSeq
//              Record07ff0f6ff4644ebeadc4697bd56a2d39(key.o_orderdate, key.o_custkey, key.Order_index, grp)
//          }.as[Record07ff0f6ff4644ebeadc4697bd56a2d39]
//
//          val x35 = x34
//          val orders = x35
//          //orders.cache
//          //orders.count
//          val x37 = Customer.select("c_name", "c_custkey")
//
//            .as[Recordea3dc6f9b6ed4825a085a971a8423122]
//
//          val x38 = x37.withColumn("Customer_index", monotonically_increasing_id())
//            .as[Record9f1664be1da24f109f1aea1ab3e17c27]
//
//          val x40 = orders
//
//
//          val x43 = x38.equiJoin(x40,
//            Seq("c_custkey"), Seq("o_custkey"), "left_outer").as[Record53d1eaf1461b4a32adb18c8ef293ff5e]
//
//          val x45 = x43.select("Customer_index", "c_name", "o_orderdate", "o_parts")
//
//            .as[Recordfbdaef1028e84a7f828a554c285e6e01]
//
//          val x47 = x45.groupByKey(x46 => Record16e3a795c8c148c0a5a11f439e060620(x46.c_name, x46.Customer_index)).mapGroups {
//            case (key, value) =>
//              val grp = value.flatMap(x46 =>
//                (x46.o_orderdate, x46.o_parts) match {
//                  case (None, _) => Seq()
//                  case (_, None) => Seq()
//                  case _ => Seq(Record93f64805c41d4615a2e3ea7d2dc10155(x46.o_orderdate match { case Some(x) => x; case _ => "null" }, x46.o_parts match { case Some(x) => x; case _ => null }))
//                }).toSeq
//              Record1b47e1a717b84648887c65f20c823179(key.Customer_index, key.c_name, grp)
//          }.as[Record1b47e1a717b84648887c65f20c823179]
//
//          val x48 = x47
//          val Test2 = x48
//
//          Test2.printSchema()
//          Test2.show(false)
//          Test2.printSchema()
        }


}
//
//
