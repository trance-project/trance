package uk.ac.ox.cs.trance

import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import framework.examples.tpch.Test2Full
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

  // For use in NestedToFlat Agg tests
  private def Test1Full(): WrappedCollection = {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()

    wrappedOrder.map { f =>

      val o_orderkey = f("o_orderkey")

      RepRow(f, "o_parts" -> wrappedLineItem.map { z =>
        val l_orderkey = z("l_orderkey")
        If(l_orderkey === o_orderkey) {
          RepRow(z)
        } {
          RepRow.empty
        }
      })
    }
  }

  private def Test2Full(): WrappedCollection = {
    val lineItem = LineItem
    val wrappedLineItem = lineItem.wrap()

    val order = Order
    val wrappedOrder = order.wrap()

    val customer = Customer
    val wrappedCustomer = customer.wrap()


    val query = wrappedCustomer.map { c =>
      RepRow(c, "c_orders" -> wrappedOrder.map { o =>
        If(c("c_custkey") === o("o_custkey")) {
          RepRow(o, "o_parts" -> wrappedLineItem.map { l =>
            If(o("o_orderkey") === l("l_orderkey")) {
              RepRow(l)
            } {
              RepRow.empty
            }
          })
        } {
          RepRow.empty
        }
      })
    }
    query
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
        RepRow(o_orderdate, "o_parts" -> wrappedLineItem.flatMap { z =>
          val l_orderkey = z("l_orderkey")
          val l_partkey = z("l_partkey")
          val l_quantity = z("l_quantity")
          If(l_orderkey === o_orderkey) {
            RepSeq(RepRow(l_partkey, l_quantity))
          } {
            RepSeq.empty
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

    it("FlatToNested Test2Full") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map{ c =>
        RepRow(c, "c_orders" -> wrappedOrder.map{o =>
          If(c("c_custkey") === o("o_custkey")) {
            RepRow(o, "o_parts" -> wrappedLineItem.map{l =>
              If(o("o_orderkey") === l("l_orderkey")) {
                RepRow(l)
              } {
                RepRow.empty
              }
            })
          }
          {
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
        RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
          If(l("l_orderkey") === o("o_orderkey")) {
            RepSeq(RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity")))
          } {
            RepSeq.empty
          }
        })
      }

      val query = wrappedCustomer.map { c =>
        RepRow("c_name" -> c("c_name"), "c_orders" -> oquery.flatMap { o =>
          If(o("o_custkey") === c("c_custkey")) {
            RepSeq(RepRow("o_orderdate" -> o("o_orderdate"), o("o_parts")))
          } {
            RepSeq.empty
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
          RepRow("o_orderdate" -> o("o_orderdate"), "o_parts" -> o("o_parts"))
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
            RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity"))
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
                RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity"))
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
            RepRow("p_name" -> p("p_name"), "total" -> l("l_quantity") * p("p_retailprice"))
          } {
            RepRow.empty
          }
        }
      }.groupBy("p_name").sum("total").leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("Test1Agg1") {

      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val orders = Test1Full()
      //TODO - solution with partRef separate

      val parts = orders.map{orderRef =>
        orderRef("o_parts").map(o => RepRow(o("l_quantity"), o("l_partkey")))
      }

      //      val partRef = orderRef.map{p =>
      //        p("o_parts").map(f => RepRow(f("l_quantity"), f("l_partkey")))
      //      }

      val query = orders.map { f =>
        f("o_parts").map { o =>
          wrappedPart.map { pr =>
            If(o("l_partkey") === pr("p_partkey")) {
              RepRow("o_orderdate" -> f("o_orderdate"), "total" -> o("l_quantity") * pr("p_retailprice"))
            } {
              RepRow.empty
            }
          }
        }
      }.groupBy("o_orderdate").sum("total").leaveNRC()

      query.show(false)
      query.printSchema()
    }

    // TODO - Finish
    it("Test1Agg1S") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val orders = Test1Full()
//      val partRef = orders.map(f => f("o_parts").map(p => RepRow(p("l_partkey"), p("l_quantity"))))
//      val parts = orders.map{p =>
//        p("o_parts").map(f => RepRow(f("l_quantity"), f("l_partkey")))
//      }
//      partRef.leaveNRC().show()

      val step1 = orders.map { orderRef =>
        orderRef("o_parts").map { p =>
          wrappedPart.map { pr =>
            If(p("l_partkey") === pr("p_partkey")) {
              RepRow("subtotal" -> p("l_quantity") * pr("p_retailprice"))
            } {
              RepRow.empty
            }
          }
        }
      }.groupBy().sum("subtotal")


      val step1SchemaTest = step1.schema
//
//      step1.show(false)
//      step1.printSchema()
//

      val query = step1.map{s =>
        orders.map{or =>
          RepRow("o_orderdate" -> or("o_orderdate"), "total" -> s("subtotal"))
        }
      }.groupBy("o_orderdate").sum("total").leaveNRC()

      query.show(false)

    }

    it("Test2Agg2") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val customers: WrappedCollection = Test2Full()

//      val orders: WrappedCollection = customers.select("c_orders")
//
//      val parts = orders.map(orderRef => orderRef("c_orders").map(c => c("o_parts").map(o => RepRow("c_name" -> orderRef("c_name"), "l_partkey" -> o("l_partkey"), "l_quantity" -> o("l_quantity")))))


      val query = customers.map{customerRef =>
        customerRef("c_orders").map{c =>
          c("o_parts").map{o =>
            wrappedPart.flatMap{pr =>
              If(o("l_partkey") === pr("p_partkey")) {
                RepSeq(RepRow("c_name" -> customerRef("c_name"), "total" -> o("l_quantity") * pr("p_retailprice"), o))
              } {
                RepSeq.empty
              }
            }
          }
        }
      }.groupBy("c_name").sum("total").leaveNRC()


      query.show(false)
      query.printSchema()


    }

//    it()
  }
}
