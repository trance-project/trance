package uk.ac.ox.cs.trance

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import uk.ac.ox.cs.trance.utilities.TPCHDataframes._

class TpchQueryTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {


  def assertDataFrameEquals(expected: DataFrame, result: DataFrame): Unit = {
    assert(expected.collect() sameElements result.collect())
  }

  def assertDataFrameSchemaEqual(df1: DataFrame, df2: DataFrame): Unit = {
    val schema1 = df1.schema.fields.map(f => f.copy(nullable = false))
    val schema2 = df2.schema.fields.map(f => f.copy(nullable = false))

    schema1 shouldEqual schema2
  }

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

  describe("FlatToNested") {
    it("FlatToNested - Test1") {
      val lineItem = LineItem

      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val oquery = wrappedOrder.map { f =>
        val o_orderdate = f("o_orderdate")
        val o_orderkey = f("o_orderkey")
        RepRow(o_orderdate, "o_parts" -> wrappedLineItem.map { z =>
          val l_orderkey = z("l_orderkey")
          val l_partkey = z("l_partkey")
          val l_quantity = z("l_quantity")
          If(l_orderkey === o_orderkey) {
            RepRow(l_partkey, l_quantity)
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      oquery.show()
      oquery.printSchema()
    }

    it("FlatToNested Test1Full") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val oquery = wrappedOrder.map { f =>

        val o_orderkey = f("o_orderkey")

        RepRow(f, "o_parts" -> wrappedLineItem.map { z =>
          val l_orderkey = z("l_orderkey")
          If(l_orderkey === o_orderkey) {
            RepRow(z)
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      oquery.show(false)
      oquery.printSchema()

    }

    it("FlatToNested Test2") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map { c =>
        val c_custkey = c("c_custkey")
        val c_name = c("c_name")
        RepRow(c_name, "o_parts" -> wrappedOrder.map { o =>
          val o_orderkey = o("o_orderkey")
          val o_custkey = o("o_custkey")
          val o_orderdate = o("o_orderdate")
          If(c_custkey === o_custkey) {
            RepRow(o_orderdate, "c_orders" -> wrappedLineItem.map { l =>
              val l_orderkey = l("l_orderkey")
              val l_partkey = l("l_partkey")
              val l_quantity = l("l_quantity")
              If(l_orderkey === o_orderkey) {
                RepRow("l_partkeyTEST" -> l_partkey, l_quantity)
              } {
                RepRow.empty
              }
            })
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("FlatToNested Test2Filter") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val query = wrappedCustomer.map { c =>
        val c_custkey = c("c_custkey")
        val c_name = c("c_name")
        val c_nationkey = c("c_nationkey")
        RepRow(c_name, "c_orders" -> wrappedOrder.map { o =>
          val o_orderkey = o("o_orderkey")
          val o_custkey = o("o_custkey")
          val o_orderdate = o("o_orderdate")
          If(c_custkey === o_custkey && c_nationkey === 1) {
            RepRow(o_orderdate, "o_parts" -> wrappedLineItem.map { l =>
              val l_orderkey = l("l_orderkey")
              val l_partkey = l("l_partkey")
              val l_quantity = l("l_quantity")
              If(l_orderkey === o_orderkey) {
                RepRow(l_partkey, l_quantity)
              } {
                RepRow.empty
              }
            })
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("FlatToNested Test2Flat") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val oquery = wrappedOrder.map { o =>
        RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.map { l =>
          If(l("l_orderkey") === o("o_orderkey")) {
            RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity"))
          } {
            RepRow.empty
          }
        })
      }

      //
      //        .leaveNRC()
      //
      //      oquery.show(false)
      //      oquery.printSchema()
      //      val wrappedOquery = oquery.wrap()
      //      val wrappedOQuerySchema = wrappedOquery.schema
      //      println(wrappedOQuerySchema)


      ////


      val query = wrappedCustomer.map { c =>
        RepRow("c_name" -> c("c_name"), "c_orders" -> oquery.map { o =>
          If(o("o_custkey") === c("c_custkey")) {
            RepRow("o_orderdate" -> o("o_orderdate"), o("o_parts"))
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()


      query.show(false)
      query.printSchema()


    }
    it("FlatToNested Test2Flat W/o problem If") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val oquery = wrappedOrder.map { o =>
        RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.map { l =>
          If(l("l_orderkey") === o("o_orderkey")) {
            RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity"))
          } {
            RepRow.empty
          }
        })
      }


      val query = wrappedCustomer.map { c =>

        RepRow("c_name" -> c("c_name"), "c_orders" -> oquery.map { o =>
          RepRow("o_orderdate" -> o("o_orderdate"), o("o_parts"))
        })
      }.leaveNRC()

      query.show(false)

    }

    it("FlatToNested Test2Flat As One Query") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map { c =>

        RepRow("c_name" -> c("c_name"), "c_orders" -> wrappedOrder.map { o =>
          If(c("c_custkey") === o("o_custkey")) {
            RepRow("o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.map { l =>
              If(l("l_orderkey") === o("o_orderkey")) {
                RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity"))
              } {
                RepRow.empty
              }
            })
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      query.show(false)

    }

    it("FlatToNested Test3") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val nation = Nation
      val wrappedNation = nation.wrap()


      val query = wrappedNation.map { n =>
        val n_name = n("n_name")
        RepRow(n_name, "n_custs" -> wrappedCustomer.map { c =>
          If(n("n_nationkey") === c("c_nationkey")) {
            RepRow(c("c_name"), "c_orders" -> wrappedOrder.map { o =>
              If(c("c_custkey") === o("o_custkey")) {
                RepRow(o("o_orderdate"), "o_parts" -> wrappedLineItem.map { l =>
                  If(o("o_orderkey") === l("l_orderkey")) {
                    RepRow(l("l_partkey"), l("l_quantity"))
                  } {
                    RepRow.empty
                  }
                })
              } {
                RepRow.empty
              }
            })
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("Test0Join") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      //      val query = wrappedLineItem.join(wrappedPart, wrappedLineItem("l_partkey") === wrappedPart("p_partkey")).map { j =>
      //        RepRow(j("p_name"), "l_qty" -> j("l_quantity"))
      //      }.leaveNRC()

      //
      //      val join = wrappedLineItem.join(wrappedPart, wrappedLineItem("l_partkey") === wrappedPart("p_partkey"))
      //      val query = join.select("p_name", "l_quantity").leaveNRC()


      val query = wrappedLineItem.map { l =>
        wrappedPart.map { p =>
          If(l("l_partkey") === p("p_partkey")) {
            RepRow(p("p_name"), "l_qty" -> l("l_quantity"))
          } {
            RepRow.empty
          }
        }
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test1Join") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val query = wrappedOrder.map { o =>
        RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.map { l =>
          If(o("o_orderkey") === l("l_orderkey")) {
            wrappedPart.map { p =>
              If(l("l_partkey") === p("p_partkey")) {
                RepRow(p("p_name"), "l_qty" -> l("l_quantity"))
              } {
                RepRow.empty
              }
            }
          } {
            RepRow.empty
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }
  }

  describe("NestedToFlat") {
    it("Test0Agg0") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val query = wrappedLineItem.map { l =>
        wrappedPart.map { p =>
          If(l("l_partkey") === p("p_partkey")) {
            RepRow(p("p_name"), "total" -> l("l_quantity") * p("p_retailprice"))
          } {
            RepRow.empty
          }
        }
      }.groupBy("p_name").sum("total").leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("Test1Agg1Full") {

      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val cop = COP
      val wrappedCOP = cop.wrap()



      //
      //
      ////      query.show(false)
      ////      query.printSchema()

      val orderRef = {
        wrappedOrder.map { f =>
//
//          val o_orderkey = f("o_orderkey")

          RepRow(f, "o_parts" -> wrappedLineItem.map { z =>
            val l_orderkey = z("l_orderkey")
              RepRow(z)
          })
        }
      }
      //
//              .leaveNRC()
//
//
//
//
//            orderRef.show(false)
//            orderRef.printSchema()
//
//      val query = wrappedCOP.map { c =>
//        val k = c("corders")
//
//        val o = k.map {
//          f =>
//            val inspectK = k
//            val inspectF = f
//            //            val inspectC = c
//            //            val inspectF = f("dateID")
//            RepRow(f)
//        }
//        o
//      }


      val partRef = orderRef.map { f =>
        val k = f("o_parts")

        RepRow(k)
      }.leaveNRC()

      println("Hi")

      partRef.show(false)
      partRef.printSchema()
      //
      //      val query =
      //        orderRef.map{ o =>
      //          partRef.map{ pr =>
      //            wrappedPart.map{ p =>
      //              If(pr("l_partkey") === p("p_partkey")) {
      //                RepRow("total" -> pr("l_quantity") * p("p_retailprice"))
      //              } {
      //                RepRow.empty
      //              }
      //            }
      //          }
      //        }.leaveNRC()

      //.groupBy("o_shippriority", "o_orderdate", "o_custkey", "o_orderpriority", "o_clerk", "o_orderstatus",
      //          "o_totalprice", "o_orderkey", "o_comment").sum("total").leaveNRC()

//      query.show(false)
//      query.printSchema()

    }
  }
}
