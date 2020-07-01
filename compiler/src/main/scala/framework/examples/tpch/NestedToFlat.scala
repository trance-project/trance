package framework.examples.tpch

import framework.common._
import framework.examples.Query
import framework.nrc.MaterializeNRC

/** Benchmark Query: Nested to Flat **/

object Test2Agg2 extends TPCHBase {

  val name = "Test2Agg2"
  val tbls: Set[String] = Set("L", "O", "C", "P")

  val (customers, customerRef) = varset(Test2Full.name, "c", Test2Full.program(Test2Full.name).varRef.asInstanceOf[BagExpr])
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))

  val query = ReduceByKey(ForeachUnion(customerRef, customers,
    ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
      ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
        ForeachUnion(pr, relP,
          IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
            Singleton(Tuple("c_name" -> customerRef("c_name"), "total" -> partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric))))))),
      List("c_name"), List("total"))

  val program = Program(Assignment(name, query))
}
