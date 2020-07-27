package framework.examples.tpch

import framework.common._
import framework.examples.Query
import framework.nrc.MaterializeNRC

/** Benchmark Query: Nested to Flat **/
object Test0Agg0 extends TPCHBase {

  val name = "Test0Agg0"
  val tbls: Set[String] = Set("L", "P")

  val (parts, partRef) = varset(Test0Full.name, "l", Test0Full.program(Test0Full.name).varRef.asInstanceOf[BagExpr])

  val query = ReduceByKey(
    ForeachUnion(partRef, parts,
      ForeachUnion(pr, relP,
        IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
          Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
            partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric))))),
      List("p_name"), List("total"))

  val program = Program(Assignment(name, query))
}

object Test1Agg1 extends TPCHBase {

  val name = "Test1Agg1"
  val tbls: Set[String] = Set("L", "O", "P")

  val (orders, orderRef) = varset(Test1Full.name, "o", Test1Full.program(Test1Full.name).varRef.asInstanceOf[BagExpr])
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))

  val query = ReduceByKey(
    ForeachUnion(orderRef, orders,
      ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
        ForeachUnion(pr, relP,
          IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), "total" -> 
              partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric)))))),
      List("o_orderdate"), List("total"))

  val program = Program(Assignment(name, query))
}


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

object Test2Agg2S extends TPCHBase {

  val name = "Test2Agg2S"
  val tbls: Set[String] = Set("L", "O", "C", "P")

  val (customers, customerRef) = varset(Test2Full.name, "c", Test2Full.program(Test2Full.name).varRef.asInstanceOf[BagExpr])
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))

  val step1 = ForeachUnion(customerRef, customers, 
      projectTuple(customerRef, Map("c_orders_sub" ->
        ReduceByKey(
          ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
            ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
              ForeachUnion(pr, relP,
                IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                  Singleton(Tuple("p_partkey" -> pr("p_partkey"), "subtotal" -> 
                    partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric)))))),
            List("p_partkey"),
            List("subtotal")
    )), List("c_orders", "c_acctbal", "c_nationkey", "c_custkey", "c_comment", "c_address", "c_mktsegment", "c_phone")))

  val (s1, s1Ref) = varset("step1", "s1", step1)
  val (subOrders, sorderRef) = varset("osub", "o", BagProject(s1Ref, "c_orders_sub"))

  val query = ReduceByKey(
    ForeachUnion(s1Ref, s1, 
      ForeachUnion(sorderRef, BagProject(s1Ref, "c_orders_sub"),
        Singleton(Tuple("c_name" -> s1Ref("c_name"), "total" -> sorderRef("subtotal"))))),
      List("c_name"), List("total"))

  val program = Program(Assignment("step1", step1), Assignment(name, query))

}

object Test3Agg3 extends TPCHBase {

  val name = "Test3Agg3"
  val tbls: Set[String] = Set("L", "O", "C", "P", "N")

  val (nations, nationRef) = varset(Test3Full.name, "n", Test3Full.program(Test3Full.name).varRef.asInstanceOf[BagExpr])
  val (customers, customerRef) = varset("orders", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))

  val query = ReduceByKey(
    ForeachUnion(nationRef, nations,
      ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
        ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
          ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                Singleton(Tuple("n_name" -> nationRef("n_name"), "total" -> 
                  partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric)))))))),
          List("n_name"), List("total"))

  val program = Program(Assignment(name, query))
}

object Test4Agg4 extends TPCHBase {

  val name = "Test4Agg4"
  val tbls: Set[String] = Set("L", "O", "C", "P", "N", "R")

  val (regions, regionRef) = varset(Test4Full.name, "r", Test4Full.program(Test4Full.name).varRef.asInstanceOf[BagExpr])
  val (nations, nationRef) = varset("nations", "n", BagProject(regionRef, "r_nations"))
  val (customers, customerRef) = varset("orders", "c", BagProject(nationRef, "n_custs"))
  val (orders, orderRef) = varset("orders", "o", BagProject(customerRef, "c_orders"))
  val (parts, partRef) = varset("parts", "l", BagProject(orderRef, "o_parts"))

  val query = ReduceByKey(
    ForeachUnion(regionRef, regions,
      ForeachUnion(nationRef, nations,
        ForeachUnion(customerRef, BagProject(nationRef, "n_custs"),
          ForeachUnion(orderRef, BagProject(customerRef, "c_orders"),
            ForeachUnion(partRef, BagProject(orderRef, "o_parts"),
              ForeachUnion(pr, relP,
                IfThenElse(Cmp(OpEq, partRef("l_partkey"), pr("p_partkey")),
                  Singleton(Tuple("r_name" -> regionRef("r_name"), "total" -> 
                    partRef("l_quantity").asNumeric * pr("p_retailprice").asNumeric))))))))),
            List("r_name"), List("total"))

  val program = Program(Assignment(name, query))
}
