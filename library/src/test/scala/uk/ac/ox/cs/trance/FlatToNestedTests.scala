package uk.ac.ox.cs.trance

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import uk.ac.ox.cs.trance.utilities.TPCHDataframes._
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import framework.examples.tpch.Test3Flat.{customerRef, nr, orderRef}

class FlatToNestedTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {
  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
    spark
  }

  describe("FlatToNested") {
    it("Test0") {
      LibraryFlatToNestedTests.Test0().show(false)
    }

    it("Test0Full") {
      LibraryFlatToNestedTests.Test0Full().show(false)

    }

    it("Test1") {
      LibraryFlatToNestedTests.Test1().show(false)
    }

    it("Test1Full") {
      LibraryFlatToNestedTests.Test1Full().show(false)

    }

    it("Test2") {
      val res = LibraryFlatToNestedTests.Test2()
    }

    it("Test2Filter") {
      LibraryFlatToNestedTests.Test2Filter().show(false)
    }

    it("Test2Full") {
      val res = LibraryFlatToNestedTests.Test2Full()
      res.show(false)
      res.printSchema()
    }

    it("Test2Flat") {
      LibraryFlatToNestedTests.Test2Flat().show(false)
    }

    it("Test3") {
      LibraryFlatToNestedTests.Test3().show(false)
    }

    it("Test3Full") {
      LibraryFlatToNestedTests.Test3Full().show(false)
    }

    it("Test3Flat") {
      LibraryFlatToNestedTests.Test3Flat().show(false)
    }

    it("Test3FullFlat") {
      LibraryFlatToNestedTests.Test3FullFlat().show(false)
    }

    it("Test4") {
      LibraryFlatToNestedTests.Test4().show(false)
    }

    it("Test4Full") {
      LibraryFlatToNestedTests.Test4Full().show(false)
    }

    it("Test4Flat") {
      LibraryFlatToNestedTests.Test4Flat().show(false)
    }

    it("Test4FullFlat") {
      LibraryFlatToNestedTests.Test4FullFlat().show(false)
    }

    it("Test0Join") {
      LibraryFlatToNestedTests.Test0Join().show(false)
    }

    it("Test1Join") {
      LibraryFlatToNestedTests.Test1Join().show(false)
    }

    it("Test1JoinFlat") {
      LibraryFlatToNestedTests.Test1JoinFlat().show(false)
    }

    it("Test2Join") {
      LibraryFlatToNestedTests.Test2Join().show(false)
    }

    it("Test2JoinFlat") {
      LibraryFlatToNestedTests.Test2JoinFlat().show(false)
    }

    it("Test3Join") {
      LibraryFlatToNestedTests.Test3Join().show(false)
    }

    it("Test3JoinFlat") {
      LibraryFlatToNestedTests.Test3JoinFlat().show(false)
    }

    it("Test4Join") {
      LibraryFlatToNestedTests.Test4Join().show(false)
    }

    it("Test4JoinFlat") {
      LibraryFlatToNestedTests.Test4JoinFlat().show(false)
    }

    it("TestFN0") {
      LibraryFlatToNestedTests.TestFN0().show(false)
    }

    it("TestFN1") {
      LibraryFlatToNestedTests.TestFN1().show(false)
    }

    it("TestFN2") {
      LibraryFlatToNestedTests.TestFN2().show(false)
    }

    it("TestFN2Full") {
      LibraryFlatToNestedTests.TestFN2Full().show(false)

    }

  }

}

object LibraryFlatToNestedTests {
  val customer = Customer
  val wrappedCustomer = customer.wrap()

  val lineItem = LineItem
  val wrappedLineItem = lineItem.wrap()

  val order = Order
  val wrappedOrder = order.wrap()

  val part = Part
  val wrappedPart = part.wrap()

  val nation = Nation
  val wrappedNation = nation.wrap()

  val region = Region
  val wrappedRegion = region.wrap()

  val supplier = Supplier
  val wrappedSupplier = supplier.wrap()

  def Test0() = {
    wrappedLineItem.map(f => RepRow(f("l_partkey"), f("l_quantity"))).leaveNRC()
  }

  def Test0Full() = {
    wrappedLineItem.map(f => RepRow(f)).leaveNRC()
  }

  def Test1() = {
    wrappedOrder.flatMap { f =>
      val o_orderdate = f("o_orderdate")
      val o_orderkey = f("o_orderkey")
      RepSeq(RepRow(o_orderdate, "o_parts" -> wrappedLineItem.flatMap { z =>
        val l_orderkey = z("l_orderkey")
        val l_partkey = z("l_partkey")
        val l_quantity = z("l_quantity")
        IfThen(l_orderkey === o_orderkey) {
          RepSeq(RepRow(l_partkey, l_quantity))
        }
      }))
    }.leaveNRC()
  }

  def Test1Full() = {
    wrappedOrder.map { f =>

      val o_orderkey = f("o_orderkey")

      RepRow(f, "o_parts" -> wrappedLineItem.map { z =>
        val l_orderkey = z("l_orderkey")
        IfThen(l_orderkey === o_orderkey) {
          RepSeq(RepRow(z).omit("uniqueId"))
        }
      })
    }.leaveNRC()
  }

  def Test2() = {
    wrappedCustomer.map { c =>
      RepRow("c_name" -> c("c_name"), "c_orders" -> wrappedOrder.flatMap { o =>
        IfThen(c("c_custkey") === o("o_custkey")) {
          RepSeq(RepRow("o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
            IfThen(l("l_orderkey") === o("o_orderkey")) {
              RepSeq(RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity")))
            }
          }))
        }
      })
    }.leaveNRC()

  }

  def Test2Filter() = {
    wrappedCustomer.map { c =>
      val c_custkey = c("c_custkey")
      val c_name = c("c_name")
      val c_nationkey = c("c_nationkey")
      RepRow(c_name, "c_orders" -> wrappedOrder.flatMap { o =>
        val o_orderkey = o("o_orderkey")
        val o_custkey = o("o_custkey")
        val o_orderdate = o("o_orderdate")
        IfThen(c_custkey === o_custkey && c_nationkey === 1) {
          RepSeq(RepRow(o_orderdate, "o_parts" -> wrappedLineItem.flatMap { l =>
            val l_orderkey = l("l_orderkey")
            val l_partkey = l("l_partkey")
            val l_quantity = l("l_quantity")
            IfThen(l_orderkey === o_orderkey) {
              RepSeq(RepRow(l_partkey, l_quantity))
            }
          }))
        }
      })
    }.leaveNRC()
  }

  def Test2Full() = {
    wrappedCustomer.map { c =>
      RepRow(c, "c_orders" -> wrappedOrder.flatMap { o =>
        IfThen(c("c_custkey") === o("o_custkey")) {
          RepSeq(RepRow(o, "o_parts" -> wrappedLineItem.flatMap { l =>
            IfThen(o("o_orderkey") === l("l_orderkey")) {
              RepSeq(RepRow(l).omit("uniqueId"))
            }
          }))
        }
      })
    }.leaveNRC()
  }

  def Test2Flat() = {
    val oquery = wrappedOrder.flatMap { o =>
      RepSeq(RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
        IfThen(l("l_orderkey") === o("o_orderkey")) {
          RepSeq(RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity")))
        }
      }))
    }

    wrappedCustomer.flatMap { c =>
      RepSeq(RepRow("c_name" -> c("c_name"), "c_orders" -> oquery.flatMap { o =>
        IfThen(o("o_custkey") === c("c_custkey")) {
          RepSeq(RepRow("o_orderdate" -> o("o_orderdate"), o("o_parts")))
        }
      }))
    }.leaveNRC()
  }

  def Test3() = {
    wrappedNation.map { n =>
      val n_name = n("n_name")
      RepRow(n_name, "n_custs" -> wrappedCustomer.flatMap { c =>
        IfThen(n("n_nationkey") === c("c_nationkey")) {
          RepSeq(RepRow(c("c_name"), "c_orders" -> wrappedOrder.flatMap { o =>
            IfThen(c("c_custkey") === o("o_custkey")) {
              RepSeq(RepRow(o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
                IfThen(o("o_orderkey") === l("l_orderkey")) {
                  RepSeq(RepRow(l("l_partkey"), l("l_quantity")))
                }
              }))
            }
          }))
        }
      })
    }.leaveNRC()
  }

  def Test3Full() = {
    wrappedNation.map { n =>
      RepRow(n, "n_custs" -> wrappedCustomer.flatMap { c =>
        IfThen(n("n_nationkey") === c("c_nationkey")) {
          RepSeq(RepRow(c, "c_orders" -> wrappedOrder.flatMap { o =>
            IfThen(c("c_custkey") === o("o_custkey")) {
              RepSeq(RepRow(o, "o_parts" -> wrappedLineItem.flatMap { l =>
                IfThen(o("o_orderkey") === l("l_orderkey")) {
                  RepSeq(RepRow(l).omit("uniqueId"))
                }
              }))
            }
          }))
        }
      })
    }.leaveNRC()
  }

  def Test3Flat() = {
    val oquery = wrappedOrder.flatMap { or =>
      RepSeq(RepRow(or("o_custkey"), or("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { lr =>
        IfThen(or("o_orderkey") === lr("l_orderkey")) {
          RepSeq(RepRow(lr("l_partkey"), lr("l_quantity")))
        }
      }))
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_nationkey"), cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
        }
      }))
    }

    val query = wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr("n_name"), "n_custs" -> cquery.flatMap { customerRef =>
        IfThen(nr("n_nationkey") === customerRef("c_nationkey")) {
          RepSeq(RepRow(customerRef("c_name"), customerRef("c_orders")))
        }
      }))
    }

    query.leaveNRC()
  }

  def Test3FullFlat() = {
    val oquery = wrappedOrder.flatMap { or =>
      RepSeq(RepRow(or, "o_parts" -> wrappedLineItem.flatMap { lr =>
        IfThen(or("o_orderkey") === lr("l_orderkey")) {
          RepSeq(RepRow(lr).omit("uniqueId"))
        }
      }))
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr, "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef, "o_parts" -> orderRef("o_parts")))
        }
      }))
    }

    wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr, "n_custs" -> cquery.flatMap { customerRef =>
        IfThen(nr("n_nationkey") === customerRef("c_nationkey")) {
          RepSeq(RepRow(customerRef))
        }
      }))
    }.leaveNRC()

  }

  def Test4() = {
    wrappedRegion.map { rr =>
      RepRow(rr("r_name"), "r_nations" -> wrappedNation.flatMap { nr =>
        IfThen(nr("n_regionkey") === rr("r_regionkey")) {
          RepSeq(RepRow(nr("n_name"), "n_custs" -> wrappedCustomer.flatMap { cr =>
            IfThen(nr("n_nationkey") === cr("c_nationkey")) {
              RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.flatMap { or =>
                IfThen(cr("c_custkey") === or("o_custkey")) {
                  RepSeq(RepRow(or("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { lr =>
                    IfThen(or("o_orderkey") === lr("l_orderkey")) {
                      RepSeq(RepRow(lr("l_partkey"), lr("l_quantity")))
                    }
                  }))
                }
              }))
            }
          }))
        }
      })
    }.leaveNRC()
  }

  def Test4Full() = {
    wrappedRegion.map { rr =>
      RepRow(rr, "r_nations" -> wrappedNation.flatMap { nr =>
        IfThen(nr("n_regionkey") === rr("r_regionkey")) {
          RepSeq(RepRow(nr, "n_custs" -> wrappedCustomer.flatMap { cr =>
            IfThen(nr("n_nationkey") === cr("c_nationkey")) {
              RepSeq(RepRow(cr, "c_orders" -> wrappedOrder.flatMap { or =>
                IfThen(cr("c_custkey") === or("o_custkey")) {
                  RepSeq(RepRow(or, "o_parts" -> wrappedLineItem.flatMap { lr =>
                    IfThen(or("o_orderkey") === lr("l_orderkey")) {
                      RepSeq(RepRow(lr).omit("uniqueId"))
                    }
                  }))
                }
              }))
            }
          }))
        }
      })
    }.leaveNRC()

  }

  def Test4Flat() = {
    val oquery = wrappedOrder.flatMap { or =>
      RepSeq(RepRow(or("o_custkey"), or("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { lr =>
        IfThen(or("o_orderkey") === lr("l_orderkey")) {
          RepSeq(RepRow(lr("l_partkey"), lr("l_quantity")))
        }
      }))
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_nationkey"), cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
        }
      }))
    }

    val nquery = wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr("n_regionkey"), nr("n_name"), "n_custs" -> cquery.flatMap { customerRef =>
        IfThen(nr("n_nationkey") === customerRef("c_nationkey")) {
          RepSeq(RepRow(customerRef("c_name"), customerRef("c_orders")))
        }
      }))
    }

    val query = wrappedRegion.flatMap { rr =>
      RepSeq(RepRow(rr("r_name"), "r_nations" -> nquery.flatMap { nationRef =>
        IfThen(rr("r_regionkey") === nationRef("n_regionkey")) {
          RepSeq(RepRow(nationRef("n_name"), nationRef("n_custs")))
        }
      }))
    }

    query.leaveNRC()
  }

  def Test4FullFlat() = {
    val oquery = wrappedOrder.flatMap { or =>
      RepSeq(RepRow(or, "o_parts" -> wrappedLineItem.flatMap { lr =>
        IfThen(or("o_orderkey") === lr("l_orderkey")) {
          RepSeq(RepRow(lr).omit("uniqueId"))
        }
      }))
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr, "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef))
        }
      }))
    }

    val nquery = wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr, "n_custs" -> cquery.flatMap { customerRef =>
        IfThen(nr("n_nationkey") === customerRef("c_nationkey")) {
          RepSeq(RepRow(customerRef))
        }
      }))
    }

    val query = wrappedRegion.flatMap { rr =>
      RepSeq(RepRow(rr, "r_nations" -> nquery.flatMap { nationRef =>
        IfThen(rr("r_regionkey") === nationRef("n_regionkey")) {
          RepSeq(RepRow(nationRef))
        }
      }))
    }

    query.leaveNRC()
  }

  def Test0Join() = {
    wrappedLineItem.map { l =>
      wrappedPart.flatMap { p =>
        IfThen(l("l_partkey") === p("p_partkey")) {
          RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
        }
      }
    }.leaveNRC()
  }

  def Test1Join() = {
    wrappedOrder.map { o =>
      RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
        IfThen(o("o_orderkey") === l("l_orderkey")) {
          wrappedPart.flatMap { p =>
            IfThen(l("l_partkey") === p("p_partkey")) {
              RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
            }
          }
        }
      })
    }.leaveNRC()
  }

  def Test1JoinFlat() = {
    val pquery = wrappedLineItem.map { lr =>
      wrappedPart.flatMap { pr =>
        IfThen(lr("l_partkey") === pr("p_partkey")) {
          RepSeq(RepRow(lr("l_orderkey"), pr("p_name"), "l_qty" -> lr("l_quantity")))
        }
      }
    }

    wrappedOrder.map { or =>
      RepRow(or("o_orderdate"), "o_parts" -> pquery.flatMap { partRef =>
        IfThen(or("o_orderkey") === partRef("l_orderkey")) {
          RepSeq(RepRow(partRef("p_name"), "l_qty" -> partRef("l_qty")))
        }
      })
    }.leaveNRC()
  }

  def Test2Join() = {
    wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.flatMap { o =>
        IfThen(cr("c_custkey") === o("o_custkey")) {
          RepSeq(RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
            IfThen(o("o_orderkey") === l("l_orderkey")) {
              wrappedPart.flatMap { p =>
                IfThen(l("l_partkey") === p("p_partkey")) {
                  RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
                }
              }
            }
          }))
        }
      }))
    }.leaveNRC()
  }

  def Test2JoinFlat() = {
    val pquery = wrappedLineItem.map { lr =>
      wrappedPart.flatMap { pr =>
        IfThen(lr("l_partkey") === pr("p_partkey")) {
          RepSeq(RepRow(lr("l_orderkey"), pr("p_name"), "l_qty" -> lr("l_quantity")))
        }
      }
    }

    val oquery = wrappedOrder.map { or =>
      RepRow(or("o_custkey"), or("o_orderdate"), "o_parts" -> pquery.flatMap { partRef =>
        IfThen(or("o_orderkey") === partRef("l_orderkey")) {
          RepSeq(RepRow(partRef("p_name"), "l_qty" -> partRef("l_qty")))
        }
      })
    }

    wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
        }
      }))
    }.leaveNRC()
  }

  def Test3Join() = {
    wrappedNation.flatMap { nr =>
      RepSeq(RepRow(
        nr("n_name"), "n_custs" -> wrappedCustomer.flatMap { cr =>
          IfThen(nr("n_nationkey") === cr("c_nationkey")) {
            RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.flatMap { o =>
              IfThen(cr("c_custkey") === o("o_custkey")) {
                RepSeq(RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
                  IfThen(o("o_orderkey") === l("l_orderkey")) {
                    wrappedPart.flatMap { p =>
                      IfThen(l("l_partkey") === p("p_partkey")) {
                        RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
                      }
                    }
                  }
                }))
              }
            }))
          }
        }))
    }.leaveNRC()
  }

  def Test3JoinFlat() = {
    val pquery = wrappedLineItem.map { lr =>
      wrappedPart.flatMap { pr =>
        IfThen(lr("l_partkey") === pr("p_partkey")) {
          RepSeq(RepRow(lr("l_orderkey"), pr("p_name"), "l_qty" -> lr("l_quantity")))
        }
      }
    }

    val oquery = wrappedOrder.map { or =>
      RepRow(or("o_custkey"), or("o_orderdate"), "o_parts" -> pquery.flatMap { partRef =>
        IfThen(or("o_orderkey") === partRef("l_orderkey")) {
          RepSeq(RepRow(partRef("p_name"), "l_qty" -> partRef("l_qty")))
        }
      })
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_nationkey"), cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
        }
      }))
    }

    wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr("n_name"), "n_custs" -> cquery.flatMap { customersRef =>
        IfThen(nr("n_nationkey") === customersRef("c_nationkey")) {
          RepSeq(RepRow(customersRef("c_name"), customersRef("c_orders")))
        }
      }))
    }.leaveNRC()
  }

  def Test4Join() = {
    wrappedRegion.flatMap { rr =>
      RepSeq(RepRow(rr("r_name"), "r_nations" -> wrappedNation.flatMap { nr =>
        IfThen(nr("n_regionkey") === rr("r_regionkey")) {
          RepSeq(RepRow(nr("n_name"), "n_custs" -> wrappedCustomer.flatMap { cr =>
            IfThen(nr("n_nationkey") === cr("c_nationkey")) {
              RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.flatMap { o =>
                IfThen(cr("c_custkey") === o("o_custkey")) {
                  RepSeq(RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
                    IfThen(o("o_orderkey") === l("l_orderkey")) {
                      RepSeq(RepRow("p_name" -> l("l_partkey"), "l_qty" -> l("l_quantity")))
                    }
                  }))
                }
              }))
            }
          }))
        }
      }))
    }.leaveNRC()
  }

  def Test4JoinFlat() = {
    val pquery = wrappedLineItem.map { lr =>
      wrappedPart.flatMap { pr =>
        IfThen(lr("l_partkey") === pr("p_partkey")) {
          RepSeq(RepRow(lr("l_orderkey"), pr("p_name"), "l_qty" -> lr("l_quantity")))
        }
      }
    }

    val oquery = wrappedOrder.map { or =>
      RepRow(or("o_custkey"), or("o_orderdate"), "o_parts" -> pquery.flatMap { partRef =>
        IfThen(or("o_orderkey") === partRef("l_orderkey")) {
          RepSeq(RepRow(partRef("p_name"), "l_qty" -> partRef("l_qty")))
        }
      })
    }

    val cquery = wrappedCustomer.flatMap { cr =>
      RepSeq(RepRow(cr("c_nationkey"), cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
        IfThen(cr("c_custkey") === orderRef("o_custkey")) {
          RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
        }
      }))
    }

    val nquery = wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr("n_regionkey"), nr("n_name"), "n_custs" -> cquery.flatMap { customersRef =>
        IfThen(nr("n_nationkey") === customersRef("c_nationkey")) {
          RepSeq(RepRow(customersRef("c_name"), customersRef("c_orders")))
        }
      }))
    }

    wrappedRegion.flatMap { rr =>
      RepSeq(RepRow(rr("r_name"), "r_nations" -> nquery.flatMap { nationRef =>
        IfThen(rr("r_regionkey") === nationRef("n_regionkey")) {
          RepSeq(RepRow(nationRef("n_name"), nationRef("n_custs")))
        }
      }))
    }.leaveNRC()
  }

  def TestFN0() = {
    val custs = wrappedOrder.flatMap { or =>
      wrappedCustomer.flatMap { cr =>
        IfThen(cr("c_custkey") === or("o_custkey")) {
          RepSeq(RepRow("c_orderkey" -> or("o_orderkey"),
            "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate")))
        }
      }
    }

    custs.flatMap { customersRef =>
      wrappedLineItem.flatMap { lr =>
        IfThen(lr("l_orderkey") === customersRef("c_orderkey")) {
          RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"),
            "l_quantity" -> lr("l_quantity")).omit("c_orderkey"))
        }
      }
    }.leaveNRC()

  }

  def TestFN1() = {
    val custs =
      wrappedOrder.flatMap { or =>
        wrappedCustomer.flatMap { cr =>
          IfThen(cr("c_custkey") === or("o_custkey")) {
            RepSeq(RepRow("c_orderkey" -> or("o_orderkey"), cr("c_name"), cr("c_nationkey"), or("o_orderdate")))
          }
        }
      }

    val custsKeyed = custs.flatMap { customersRef =>
      wrappedLineItem.flatMap { lr =>
        IfThen(lr("l_orderkey") === customersRef("c_orderkey")) {
          RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity")).omit("c_orderkey")) // Omit missing here
        }
      }
    }

    wrappedSupplier.flatMap { sr =>
      RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
        IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
          RepSeq(RepRow(csuppRef).omit("c_suppkey"))
        }
      }))
    }.leaveNRC()
  }

  def TestFN2() = {
    val custs =
      wrappedOrder.flatMap { or =>
        wrappedCustomer.flatMap { cr =>
          IfThen(cr("c_custkey") === or("o_custkey")) {
            RepSeq(RepRow("c_orderkey" -> or("o_orderkey"), cr("c_name"), cr("c_nationkey"), or("o_orderdate")))
          }
        }
      }

    val custsKeyed = custs.flatMap { customersRef =>
      wrappedLineItem.flatMap { lr =>
        IfThen(lr("l_orderkey") === customersRef("c_orderkey")) {
          RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity")).omit("c_orderkey"))
        }
      }
    }

    wrappedNation.flatMap { nr =>
      RepSeq(RepRow(nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> wrappedSupplier.flatMap { sr =>
        RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
          IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
            RepSeq(RepRow(csuppRef).omit("c_suppkey"))
          }
        }))
      }))
    }.leaveNRC()

  }

  def TestFN2Full() = {
    val custs =
      wrappedOrder.flatMap { or =>
        wrappedCustomer.flatMap { cr =>
          IfThen(cr("c_custkey") === or("o_custkey")) {
            RepSeq(RepRow("c_orderkey" -> or("o_orderkey"), cr("c_name"), cr("c_nationkey"), or("o_orderdate")))
          }
        }
      }

    val custsKeyed = custs.flatMap { customersRef =>
      wrappedLineItem.flatMap { lr =>
        IfThen(lr("l_orderkey") === customersRef("c_orderkey")) {
          RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity")).omit("c_orderkey"))
        }
      }
    }

    val suppsKeyed = wrappedSupplier.flatMap { sr =>
      RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
        IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
          RepSeq(RepRow(csuppRef).omit("c_suppkey"))
        }
      }))
    }

    wrappedNation.flatMap { nr =>
      RepSeq(RepRow("n_name" -> nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> suppsKeyed.flatMap { suppliersRef =>
        IfThen(suppliersRef("s_nationkey") === nr("n_nationkey")) {
          RepSeq(RepRow(suppliersRef).omit("s_nationkey"))
        }
      }))
    }.leaveNRC()
  }

}
