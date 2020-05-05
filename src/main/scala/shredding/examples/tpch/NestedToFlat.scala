package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.MaterializeNRC

/** Benchmark Query: Nested to Flat **/

object Test2Agg2 extends TPCHBase {

  val name = "Test2Agg2"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"c__Dc_orders_1", s"o__Do_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
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