package uk.ac.ox.cs.trance

import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import uk.ac.ox.cs.trance.utilities.TPCHDataframes._
import uk.ac.ox.cs.trance.utilities.{JoinContext, Symbol}
import Wrapper.DataFrameImplicit
import framework.examples.tpch.Test3Flat.{customerRef, nr, orderRef}


class FlatToNestedTests extends AnyFunSpec with BeforeAndAfterEach with Serializable {

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

  override protected def afterEach(): Unit = {
    Symbol.freshClear()
    JoinContext.freshClear()
    super.afterEach()
  }

  describe("FlatToNested") {
    it("Test0") {
      val lineItem: WrappedCollection = LineItem.wrap()

      val query = lineItem.map(f => RepRow(f("l_partkey"), f("l_quantity"))).leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test0Full") {
      val lineItem: WrappedCollection = LineItem.wrap()

      val query = lineItem.map(f => RepRow(f)).leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test1") {
      val lineItem = LineItem

      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val oquery = wrappedOrder.flatMap { f =>
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
          IfThen(l_orderkey === o_orderkey) {
            RepSeq(RepRow(z))
          }
        })
      }.leaveNRC()

      oquery.show(false)
      oquery.printSchema()

    }

    it("Test2") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map { c =>
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

      query.show(false)
      query.printSchema()

    }

    it("Test2Filter") {
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

      query.show(false)
      query.printSchema()

    }

    it("Test2Full") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map { c =>
        RepRow(c, "c_orders" -> wrappedOrder.flatMap { o =>
          IfThen(c("c_custkey") === o("o_custkey")) {
            RepSeq(RepRow(o, "o_parts" -> wrappedLineItem.flatMap { l =>
              IfThen(o("o_orderkey") === l("l_orderkey")) {
                RepSeq(RepRow(l))
              }
            }))
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    // TODO - Bug
    it("Test2Flat") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val oquery = wrappedOrder.map { o =>
        RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
          IfThen(l("l_orderkey") === o("o_orderkey")) {
            RepSeq(RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity")))
          }
        })
      }

      val query = wrappedCustomer.map { c =>
        RepRow("c_name" -> c("c_name"), "c_orders" -> oquery.flatMap { o =>
          IfThen(o("o_custkey") === c("c_custkey")) {
            RepSeq(RepRow("o_orderdate" -> o("o_orderdate"), o("o_parts")))
          }
        })
      }.leaveNRC()


      query.show(false)
      query.printSchema()


    }

    it("Test2Flat W/o problem IfThen") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val oquery = wrappedOrder.map { o =>
        RepRow("o_custkey" -> o("o_custkey"), "o_orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
          IfThen(l("l_orderkey") === o("o_orderkey")) {
            RepSeq(RepRow("l_partkey" -> l("l_partkey"), "l_quantity" -> l("l_quantity")))
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

    it("Test2Flat As One Query") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()


      val query = wrappedCustomer.map { c =>

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

      query.show(false)

    }

    it("Test3") {
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

      query.show(false)
      query.printSchema()

    }

    it("Test3Full") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val nation = Nation
      val wrappedNation = nation.wrap()

      val query = wrappedNation.map { n =>
        RepRow(n, "n_custs" -> wrappedCustomer.flatMap { c =>
          IfThen(n("n_nationkey") === c("c_nationkey")) {
            RepSeq(RepRow(c, "c_orders" -> wrappedOrder.flatMap { o =>
              IfThen(c("c_custkey") === o("o_custkey")) {
                RepSeq(RepRow(o, "o_parts" -> wrappedLineItem.flatMap { l =>
                  IfThen(o("o_orderkey") === l("l_orderkey")) {
                    RepSeq(RepRow(l))
                  }
                }))
              }
            }))
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()

    }

    // TODO 'Flat' tests are bugged
    it("Test3Flat") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val nation = Nation
      val wrappedNation = nation.wrap()

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

      val res = query.leaveNRC()
      res.show(false)
      res.printSchema()
    }

    it("Test3FullFlat") {
      // TODO
    }

    it("Test4") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val nation = Nation
      val wrappedNation = nation.wrap()

      val region = Region
      val wrappedRegion = region.wrap()

      val query = wrappedRegion.map { rr =>
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

      query.show(false)
      query.printSchema()
    }

    it("Test4Full") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val nation = Nation
      val wrappedNation = nation.wrap()

      val region = Region
      val wrappedRegion = region.wrap()

      val query = wrappedRegion.map { rr =>
        RepRow(rr, "r_nations" -> wrappedNation.flatMap { nr =>
          IfThen(nr("n_regionkey") === rr("r_regionkey")) {
            RepSeq(RepRow(nr, "n_custs" -> wrappedCustomer.flatMap { cr =>
              IfThen(nr("n_nationkey") === cr("c_nationkey")) {
                RepSeq(RepRow(cr, "c_orders" -> wrappedOrder.flatMap { or =>
                  IfThen(cr("c_custkey") === or("o_custkey")) {
                    RepSeq(RepRow(or, "o_parts" -> wrappedLineItem.flatMap { lr =>
                      IfThen(or("o_orderkey") === lr("l_orderkey")) {
                        RepSeq(RepRow(lr))
                      }
                    }))
                  }
                }))
              }
            }))
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test4Flat") {
      // TODO
    }

    it("Test4FullFlat") {
      // TODO
    }

    it("Test0Join") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val query = wrappedLineItem.map { l =>
        wrappedPart.flatMap { p =>
          IfThen(l("l_partkey") === p("p_partkey")) {
            RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
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

      query.show(false)
      query.printSchema()
    }

    it("Test1JoinFlat") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val pquery = wrappedLineItem.map { lr =>
        wrappedPart.flatMap { pr =>
          IfThen(lr("l_partkey") === pr("p_partkey")) {
            RepSeq(RepRow(lr("l_orderkey"), pr("p_name"), "l_qty" -> lr("l_quantity")))
          }
        }
      }

      val query = wrappedOrder.map { or =>
        RepRow(or("o_orderdate"), "o_parts" -> pquery.flatMap { partRef =>
          IfThen(or("o_orderkey") === partRef("l_orderkey")) {
            RepSeq(RepRow(partRef("p_name"), "l_qty" -> partRef("l_qty")))
          }
        })
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test2Join") {
      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val part = Part
      val wrappedPart = part.wrap()

      val query = wrappedCustomer.flatMap { cr =>
        RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.map { o =>
          RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
            IfThen(o("o_orderkey") === l("l_orderkey")) {
              wrappedPart.flatMap { p =>
                IfThen(l("l_partkey") === p("p_partkey")) {
                  RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
                }
              }
            }
          })
        }))
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    // TODO - Nested IfThen Bug in Flat Test
    it("Test2JoinFlat") {
      val lineItem = LineItem
      val wrappedLineItem = lineItem.wrap()

      val order = Order
      val wrappedOrder = order.wrap()

      val customer = Customer
      val wrappedCustomer = customer.wrap()

      val part = Part
      val wrappedPart = part.wrap()

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

      val query = wrappedCustomer.flatMap { cr =>
        RepSeq(RepRow(cr("c_name"), "c_orders" -> oquery.flatMap { orderRef =>
          IfThen(cr("c_custkey") === orderRef("o_custkey")) {
            RepSeq(RepRow(orderRef("o_orderdate"), "o_parts" -> orderRef("o_parts")))
          } 
        }))
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test3Join") {
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

      val query =
        wrappedNation.flatMap { nr =>
          RepSeq(RepRow(
            nr("n_name"), "n_custs" -> wrappedCustomer.flatMap { cr =>
              RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.map { o =>
                RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
                  IfThen(o("o_orderkey") === l("l_orderkey")) {
                    wrappedPart.flatMap { p =>
                      IfThen(l("l_partkey") === p("p_partkey")) {
                        RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
                      }
                    }
                  }
                })
              }))
            }))
        }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Test3JoinFlat") {
      // TODO
    }

    it("Test4Join") {
      val query = {
        wrappedRegion.flatMap { rr =>
          RepSeq(RepRow(rr("r_name"), "r_nations" -> wrappedNation.flatMap { nr =>
            RepSeq(RepRow(nr("n_name"), "n_custs" -> wrappedCustomer.flatMap { cr =>
              RepSeq(RepRow(cr("c_name"), "c_orders" -> wrappedOrder.map { o =>
                RepRow("orderdate" -> o("o_orderdate"), "o_parts" -> wrappedLineItem.flatMap { l =>
                  IfThen(o("o_orderkey") === l("l_orderkey")) {
                    wrappedPart.flatMap { p =>
                      IfThen(l("l_partkey") === p("p_partkey")) {
                        RepSeq(RepRow("p_name" -> p("p_name"), "l_qty" -> l("l_quantity")))
                      }
                    }
                  }
                })
              }))
            }))
          }))
        }.leaveNRC()
      }

      query.show(false)
      query.printSchema()
    }

    it("Test4JoinFlat") {
      // TODO
    }

    it("TestFN0") {
      val custs = wrappedOrder.flatMap{ or =>
        wrappedCustomer.flatMap{cr =>
          IfThen(cr("c_custkey") === or("o_custkey")) {
            RepSeq(RepRow("c_orderkey" -> or("o_orderkey"),
              "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "o_orderdate" -> or("o_orderdate")))
          } 
        }
      }

      val query = custs.flatMap{customersRef =>
        wrappedLineItem.flatMap{lr =>
          IfThen(lr("l_orderkey") === customersRef("c_orderkey")) {
            RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"),
              "l_quantity" -> lr("l_quantity")))
          } 
        }.drop("c_orderkey")
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("TestFN1") {
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
            RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity"))) // Omit missing here
          } 
        }.drop("c_orderkey") // Nested Drop problem
      }

      val query = wrappedSupplier.flatMap { sr =>
          RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
            IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
              RepSeq(RepRow(csuppRef))
            }
          }))
      }.drop("c_suppkey").leaveNRC()

      query.show(false)
      query.printSchema()

    }

    it("TestFN2") {
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
            RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity"))) // Omit missing here
          }
        }
      }

      val query = wrappedNation.flatMap { nr =>
        RepSeq(RepRow(nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> wrappedSupplier.flatMap { sr =>
          RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
            IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
              RepSeq(RepRow(csuppRef)) // Missing omit
            }
          }.drop("")))
        }))
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    // TODO nested IF bug
    it("TestFN2Full") {
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
            RepSeq(RepRow(customersRef, "c_suppkey" -> lr("l_suppkey"), "l_partkey" -> lr("l_partkey"), "l_quantity" -> lr("l_quantity"))) // Omit missing here
          }
        }
      }

      val suppsKeyed = wrappedSupplier.flatMap { sr =>
          RepSeq(RepRow(sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> custsKeyed.flatMap { csuppRef =>
            IfThen(csuppRef("c_suppkey") === sr("s_suppkey")) {
              RepSeq(RepRow(csuppRef)) // Missing omit c_suppkey
            }
       }))
      }

      val query = wrappedNation.flatMap{ nr =>
        RepSeq(RepRow("n_name" -> nr("n_name"), "n_nationkey" -> nr("n_nationkey"), "n_suppliers" -> suppsKeyed.flatMap{suppliersRef =>
          IfThen(suppliersRef("s_nationkey") === nr("n_nationkey")) {
            RepSeq(RepRow(suppliersRef)) //Omit s_nationkey
          }
        }))
      }.leaveNRC()

      query.show(false)
      query.printSchema()
    }

    it("Quick Drop Test") {
      val query = wrappedNation.drop("n_name").leaveNRC()

      query.show(false)
      query.printSchema()

    }
  }

}
