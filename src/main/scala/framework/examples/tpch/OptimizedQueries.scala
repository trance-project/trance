package framework.examples.tpch

import framework.core._
import framework.utils.Utils.Symbol

/**
  This file contains exploratory queries on tpch 
**/

object TPCHQueryCustOrders extends TPCHBase {
  val name = "CustOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  // input data
  val queryCO = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "corders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate"))))))))

  val program = Program(Assignment(name, queryCO))
}

object TPCHQuery4A extends TPCHBase {
  val name = "Query4"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1r, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val cor2 = TupleVarRef("order", BagProject(cor, "corders").tp.tp)

  val parts = ForeachUnion(lr, relL,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1r, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])
  val query4 =
    ForeachUnion(cor, q1r,
      Singleton(Tuple("c_name" -> cor("c_name"), "partqty" ->
        ReduceByKey(
          ForeachUnion(cor2, BagProject(cor, "corders"),
            ForeachUnion(cor1, p1r,
              IfThenElse(Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
                Singleton(Tuple("orderdate" -> cor2("o_orderdate"),
                  "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty")))))),
          List("orderdate", "pname"),
          List("l_qty")
        ))))

  val program = Program(Assignment(p1r.name, parts), Assignment(name, query4))
}

object TPCHQuery4PartOrders extends TPCHBase {
  val name = "PartOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2orders_1")

  // input data
  val query4 = ForeachUnion(lr, relL,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"), "orders" -> 
                      ForeachUnion(or, relO,
                        IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("o_custkey" -> or("o_custkey"), "orderdate" -> or("o_orderdate"))))))))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4B extends TPCHBase {
  val name = "Query4"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2orders_1")

  val (q1, cor) = varset(TPCHQuery4PartOrders.name, "part",
    TPCHQuery4PartOrders.program(TPCHQuery4PartOrders.name).varRef.asInstanceOf[BagExpr])

  val cor2 = TupleVarRef("order", BagProject(cor, "orders").tp.tp)

  val query4 =
      ReduceByKey(
        ForeachUnion(cor, BagVarRef(q1.name, q1.tp.asInstanceOf[BagType]),
          ForeachUnion(cor2, BagProject(cor, "orders"),
            ForeachUnion(cr, relC,
              IfThenElse(
                Cmp(OpEq, cr("c_custkey"), cor2("o_custkey")),
                Singleton(Tuple("c_name" -> cr("c_name"), "p_name" -> cor("p_name"), "l_qty" -> cor("l_qty"))))))),
        List("c_name", "p_name"),
        List("l_qty")
      )

  val program = Program(Assignment(name, query4))
}


// nested to flat that does an intermedite join
object TPCHQuery4C extends TPCHBase {
  val name = "Query4C"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1r, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val cor2 = TupleVarRef("order", BagProject(cor, "corders").tp.tp)

  val parts = ForeachUnion(lr, relL,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1r, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])
  val query4 =
    ReduceByKey(
      ForeachUnion(cor, q1r,
        ForeachUnion(cor2, BagProject(cor, "corders"),
          ForeachUnion(cor1, p1r,
            IfThenElse(
              Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
              Singleton(Tuple("c_name" -> cor("c_name"), "orderdate" -> cor2("o_orderdate"),
                "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty"))))))),
      List("c_name", "orderdate", "pname"), 
      List("l_qty")
    )

  val program = Program(Assignment(p1r.name, parts), Assignment(name, query4))
}

object TPCHQuery4D extends TPCHBase {
  val name = "Query4D"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1, cor) = varset(TPCHQueryCustOrders.name, "customer",
    TPCHQueryCustOrders.program(TPCHQueryCustOrders.name).varRef.asInstanceOf[BagExpr])

  val cor2 = TupleVarRef("order", BagProject(cor, "corders").tp.tp)

  val parts = ForeachUnion(lr, relL,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                    Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  val (p1r, cor1) = varset("parts", "part", parts.asInstanceOf[BagExpr])

  val query4 =
    ReduceByKey(
      ForeachUnion(cor, p1r,
        ForeachUnion(cor2, BagProject(cor, "corders"),
          ForeachUnion(cor1,p1r,
            IfThenElse(
              Cmp(OpEq, cor2("o_orderkey"), cor1("l_orderkey")),
              Singleton(Tuple("orderdate" -> cor2("o_orderdate"),
                          "pname" -> cor1("p_name"), "l_qty" -> cor1("l_qty"))))))),
      List("orderdate", "pname"), 
      List("l_qty")
    )

  val program = Program(Assignment(p1r.name, parts), Assignment(name, query4))
}

object TPCHQueryInputs extends TPCHBase {
  val name = "CustOrders"
 
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] = 
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val parts = ForeachUnion(lr, relL,
                Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "parts" ->
                    ForeachUnion(pr, relP,
                      IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))
  val (p1r, cor2) = varset("parts", "part", parts.asInstanceOf[BagExpr])

  val queryCO = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "corders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate")))))))) 

  // input data
  val program = Program(Assignment(p1r.name, parts), Assignment(name, queryCO))
}

object TPCHQuery4E extends TPCHBase {
  val name = "Query4E"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2corders_1")

  val (q1r, cor) = varset(TPCHQueryInputs.name, "customer",
    TPCHQueryInputs.program(TPCHQueryInputs.name).varRef.asInstanceOf[BagExpr])

  val cor1 = TupleVarRef("order", BagProject(cor, "corders").tp.tp)

  val (p1r, cor2) = varset("parts", "part", TPCHQueryInputs.parts.asInstanceOf[BagExpr])
  val cor3 = TupleVarRef("q", BagProject(cor2, "parts").tp.tp)

  //val query = Sequence(List(
 //   Named(p1, parts),
  val query4 =
    ReduceByKey(
      ForeachUnion(cor, q1r,
        ForeachUnion(cor1, BagProject(cor, "corders"),
          ForeachUnion(cor2, p1r,
          IfThenElse(Cmp(OpEq, cor1("o_orderkey"), cor2("l_orderkey")),
            ForeachUnion(cor3, BagProject(cor2, "parts"),
              Singleton(Tuple("c_name" -> cor("c_name"), "orderdate" -> cor1("o_orderdate"), 
              "pname" -> cor3("p_name"), "l_qty" -> cor3("l_qty")))))))),
      List("c_name", "orderdate", "pname"), 
      List("l_qty")
    )

  val program = Program(Assignment(name, query4))
}

object TPCHQuery7A extends TPCHBase {
  val name = "Query7"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x =>
      List("C", "O", "L", "P", "PS", "S", "N").contains(x._1)).values.toList.mkString("")}"

  val (q3r, cor) = varset(TPCHQuery3Full.name, "co",
    TPCHQuery3Full.program(TPCHQuery3Full.name).varRef.asInstanceOf[BagExpr])

  val customers = BagProject(cor, "customers")
  val c2r = TupleVarRef(Symbol.fresh(), customers.tp.tp)

  val suppliers = BagProject(cor, "suppliers")
  val s2r = TupleVarRef(Symbol.fresh(), suppliers.tp.tp)

  val query7 = ForeachUnion(nr, relN,
                Singleton(Tuple("n_name" -> nr("n_name"), "parts" ->
                  ForeachUnion(cor, q3r,
                    Singleton(Tuple("p_name" -> cor("p_name"), "suppliers" ->
                      ForeachUnion(s2r, suppliers,
                        IfThenElse(Cmp(OpEq, s2r("s_nationkey"), nr("n_nationkey")),
                          Singleton(Tuple("s_name" -> s2r("s_name"))))), "customers" -> 
                      ForeachUnion(c2r, customers,
                        IfThenElse(Cmp(OpEq, c2r("c_nationkey"), nr("n_nationkey")),
                          Singleton(Tuple("c_name" -> c2r("c_name")))))))))))

  val program = Program(Assignment(name, query7))
}

object TPCHQuery4New extends TPCHBase {
  val name = "Query4New"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 = ForeachUnion(cor, q1r,
                Singleton(Tuple("c_name" -> cor("c_name"), "totals" ->
                  ReduceByKey(
                    ForeachUnion(co2r, orders,
                      ForeachUnion(co3r, parts,
                        Singleton(Tuple("orderdate" -> co2r("o_orderdate"),
                          "pname" -> co3r("p_name"), "qty" -> co3r("l_qty"))))),
                    List("orderdate", "pname"),
                    List("qty")
                  ))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4New2 extends TPCHBase {
  val name = "Query4New2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        Singleton(Tuple("orders" -> co2r("o_orderdate"), "customers" ->
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
              Singleton(Tuple("name" -> cr("c_name"), "address" -> cr("c_address")))))))))

  val program = Program(Assignment(name, query4))
}

object TPCHQuery4New3 extends TPCHBase {
  val name = "Query4New3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ReduceByKey(
      ForeachUnion(cor, q1r,
        ForeachUnion(co2r, orders,
          ForeachUnion(co3r, parts,
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                                "qty" -> co3r("l_qty"), "price" -> pr("p_retailprice")))))))),
      List("c_name", "p_name", "qty"),
      List("price")
    )

  val program = Program(Assignment(name, query4))
}

// transpose on the third level
object TPCHQuery4New4 extends TPCHBase {
  val name = "Query4New3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        ForeachUnion(co3r, parts,
          Singleton(Tuple("p_name" -> co3r("p_name"), "customers" -> 
            ForeachUnion(cr, relC,
              IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
                Singleton(Tuple("customer" -> cr("c_name"), "address" -> cr("c_address"))))))))))

  val program = Program(Assignment(name, query4))
}


