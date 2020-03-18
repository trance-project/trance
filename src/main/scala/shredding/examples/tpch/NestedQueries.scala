package shredding.examples.tpch

import shredding.core._
import shredding.utils.Utils.Symbol

/**
For c2 in Query1 Union
  Sng((c_name := c2.c_name, totals := (
    For o2 in c2.c_orders Union
      For p2 in o2.o_parts Union
        Sng((orderdate := o2.o_orderdate, pname := p2.p_name, qty := p2.l_qty))
    ).groupBy+((orderdate := x1.orderdate, pname := x1.pname)), x1.qty)))
**/
object TPCHNested1 extends TPCHBase {
  val name = "TPCHNested1"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query1 =
    ForeachUnion(cor, q1r,
      Singleton(Tuple("c_name" -> cor("c_name"), "totals" ->
        SumByKey(
          ForeachUnion(co2r, orders,
            ForeachUnion(co3r, parts,
              Singleton(Tuple("orderdate" -> co2r("o_orderdate"),
                "pname" -> co3r("p_name"), "qty" -> co3r("l_qty"))))),
          List("orderdate", "pname"),
          List("qty")
        ))))

  val program = Program(Assignment(name, query1))
}

/**
(For c2 in Query1 Union
  For o2 in c2.c_orders Union
    For p2 in o2.o_parts Union
      For p in P Union
        If (p.p_name = p2.p_name)
        Then Sng((c_name := c2.c_name, p_name := p.p_name, total := p2.l_qty*p.p_retailprice))
  ).groupBy+((c_name := x1.c_name, p_name := x1.p_name), x1.total)

Note that this query is not yet supported because it requires multiplication.
**/
object TPCHNested2 extends TPCHBase {
  val name = "TPCHNested2Unopt"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query2 =
      /**ForeachUnion(co, BagVarRef(q1),
        ForeachUnion(co2, orders,
          Singleton(Tuple("c_name" -> cor("c_name"), "parts" -> 
            GroupBy(ForeachUnion(co3, parts, 
              ForeachUnion(p, relP,
                IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                  Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                    "total" -> PrimitiveOp(Multiply, co3r("l_qty"), pr("p_retailprice"))))))),
              List("c_name", "p_name"),
              List("total"),
              DoubleType       
             )))))**/
    SumByKey(
      ForeachUnion(cor, q1r,
        ForeachUnion(co2r, orders,
          ForeachUnion(co3r, parts,
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                                "total" -> co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))))),
      List("c_name", "p_name"),
      List("total")
    )

  val program = Program(Assignment(name, query2))
}

/**
For c2 in Query1 Union
  Sng((c_name := c2.c_name, c_orders := 
    (For o2 in c2.c_orders Union
      For p2 in o2.o_parts Union
        For p in P Union
          If (p.p_name = p2.p_name)
          Then Sng((o_orderdate := o2.o_orderdate, total := p2.l_qty*p.p_retailprice))
    ).groupBy+((o_orderdate := x1.o_orderdate), x1.total)

Again, this isn't supported
**/
object TPCHNested3 extends TPCHBase {
  val name = "TPCHNested3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query3 =
    ForeachUnion(cor, q1r,
      Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
        SumByKey(
          ForeachUnion(co2r, orders,
            ForeachUnion(co3r, parts,
              ForeachUnion(pr, relP,
                IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                  Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "p_name" -> pr("p_name"), 
                                "qty" -> co3r("l_qty"), "price" -> pr("p_retailprice"))))))),
          List("o_orderdate", "p_name", "qty"),
          List("price")
        ))))

  val program = Program(Assignment(name, query3))
}

/**
For c2 in Query1 Union
  Sng((c_name := c2.c_name, c_orders := 
    For o2 in c2.c_orders Union
      Sng((o_orderdate := o2.o_orderdate, o_parts := 
        (For p2 in o2.o_parts Union
          For p in P Union
            If (p.p_name = p2.p_name)
            Then Sng((p_name := p2.p_name, total := p2.l_qty*p.p_retailprice))
    ).groupBy+((p_name := x1.p_name), x1.total)

Again, need multiplication for this to work.
**/
object TPCHNested4 extends TPCHBase {
  val name = "TPCHNested4"

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
      Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
        ForeachUnion(co2r, orders,
          Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
            SumByKey(
              ForeachUnion(co3r, parts,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                      co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))),
              List("p_name"),
              List("total")
            )))))))

  val program = Program(Assignment(name, query4))
}

object TPCHNested4Filter extends TPCHBase {
  val name = "TPCHNested4Filter"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1WK.name, "c2",
    TPCHQuery1WK.program(TPCHQuery1WK.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ForeachUnion(cor, q1r,
      IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")),
        Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
          ForeachUnion(co2r, orders,
            IfThenElse(Cmp(OpGe, Const(150000000, IntType), or("o_orderkey")),
            Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
              SumByKey(
                ForeachUnion(co3r, parts,
                  ForeachUnion(pr, relP,
                    IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                          co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))),
                List("p_name"),
                List("total")
              )))))))))

  val program = Program(Assignment(name, query4))
}

/**
For c2 in Query1 Union
  For o2 in c2.c_orders Union
    Sng((orders := o2.o_orderdate, customers := For c in C Union
      If (c.c_name = c2.c_name)
      Then Sng((name := c.c_name, address := c.c_address))))
**/
object TPCHNested5a extends TPCHBase {
  val name = "TPCHNested5a"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query5 =
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        Singleton(Tuple("orders" -> co2r("o_orderdate"), "customers" ->
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
              Singleton(Tuple("name" -> cr("c_name"), "address" -> cr("c_address")))))))))

  val program = Program(Assignment(name, query5))
}

object TPCHNested5b extends TPCHBase {
  val name = "TPCHNested5b"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)
  
  val flat = 
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        Singleton(Tuple("order" -> co2r("o_orderdate"), "customer" -> cor("c_name")))))
  val (frefr, fr) = varset("flat", "f", flat)

  val query5 =
    ForeachUnion(or, relO,
      Singleton(Tuple("order" -> or("o_orderdate"), "customers" -> 
        ForeachUnion(fr, frefr,
            IfThenElse(Cmp(OpEq, fr("order"), or("o_orderdate")),
              Singleton(Tuple("customer" -> cor("c_name"))))))))

  val program = Program(Assignment(frefr.name, flat), Assignment(name, query5))
}

/**
let flat =
  For c in C Union
    For c2 in Query1 Union
      If c2.c_name = c.c_name
      Then For o2 in c2.c_orders Union
        Sng((order := o2.orderdate, customer := c2.c_name, address := c.c_address, parts := o2.o_parts))

For o in O Union
  Sng((order := o.orderdate, customers := For f in flat Union
    If (f.order = o.orderdate)
    Then (For p in flat.parts Union
      Sng((c_name := f.c_name, address := f.address, p_name := p.p_name, qty := p.l_qty))
      ).groupBy+((x1.c_name, x1.address, x1.p_name))))
**/
object TPCHNested5 extends TPCHBase {
  val name = "TPCHNested5"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)
  
  val flat = 
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        Singleton(Tuple("order" -> co2r("o_orderdate"), "customer" -> cor("c_name")))))
  val (frefr, fr) = varset("flat", "f", flat)

  val query5 =
    ForeachUnion(or, relO,
      Singleton(Tuple("order" -> or("o_orderdate"), "customers" -> 
        ForeachUnion(fr, frefr,
            IfThenElse(Cmp(OpEq, fr("order"), or("o_orderdate")),
              Singleton(Tuple("customer" -> cor("c_name"))))))))

  val program = Program(Assignment(frefr.name, flat), Assignment(name, query5))
}


/**
For c2 in Query1 Union
  For o2 in c2.c_orders Union
    For p2 in o2.o_parts Union
      Sng((p_name := p2.p_name, customers := For c in C Union
        If (c.c_name = c2.c_name)
        Then Sng((customer := c.c_name, address := c.c_address))))
**/
object TPCHNested6 extends TPCHBase {
  val name = "TPCHNested6"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(TPCHQuery1Full.name, "c2",
    TPCHQuery1Full.program(TPCHQuery1Full.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query6 =
    ForeachUnion(cor, q1r,
      ForeachUnion(co2r, orders,
        ForeachUnion(co3r, parts,
          Singleton(Tuple("p_name" -> co3r("p_name"), "customers" ->
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_name"), cor("c_name")),
              Singleton(Tuple("name" -> cr("c_name"), "address" -> cr("c_address"))))))))))

  val program = Program(Assignment(name, query6))
}

//object TPCHNested7 = TPCHQuery6

/**
cflat := For co in Query2 Union
  For co2 in co.customers2 Union
    Sng((c_name := co2.c_name2, s_name := co.s_name))

For c in C Union
  Sng((c_name := c.c_name, suppliers := Total(For cf in cflat Union
    If (cf.c_name = c.c_name)
    Then Sng((s_name := cf.s_name)))))
**/
object TPCHNested8 extends TPCHBase {
  val name = "TPCHNested8"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2r, cor) = varset(TPCHQuery2Full.name, "co",
    TPCHQuery2Full.program(TPCHQuery2Full.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)
  
  val flat = ForeachUnion(cor, q2r,
              ForeachUnion(co2r, cust,
                Singleton(Tuple("c_name" -> co2r("c_name2"), "s_name" -> cor("s_name")))))
  val (cflatr, cfr) = varset("cflat", "cf", flat.asInstanceOf[BagExpr])
  val query8 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  Count(ForeachUnion(cfr, cflatr,
                    IfThenElse(Cmp(OpEq, cfr("c_name"), cr("c_name")),
                      Singleton(Tuple("s_name" -> cfr("s_name")))))))))

  val program = Program(Assignment(cflatr.name, flat), Assignment(name, query8))
}

/**
For co in Query2 Union
  Sng((s_name := co.s_name, nations := 
    For n in N Union 
      Sng((n_name:= n.n_name, customers := 
          For co2 in co.customers2 Union 
            For c in C Union
              If (n.n_nationkey = c.c_nationkey)
              Then Sng((c_name:= c.c_name))))))
**/
object TPCHNested9 extends TPCHBase {
  val name = "TPCHNested9"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2r, cor) = varset(TPCHQuery2Full.name, "co",
    TPCHQuery2Full.program(TPCHQuery2Full.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)
  
  val query9 =
    ForeachUnion(cor, q2r,
      Singleton(Tuple("s_name" -> cor("s_name"), "nations" -> 
        ForeachUnion(nr, relN,
          Singleton(Tuple("nation" -> nr("n_name"), "customers" ->
            ForeachUnion(co2r, cust,
              IfThenElse(And(
                Cmp(OpEq, nr("n_nationkey"), cr("c_nationkey")),
                Cmp(OpEq, co2r("c_name2"), cr("c_name"))),
                Singleton(Tuple("c_name" -> cr("c_name")))))))))))

  val program = Program(Assignment(name, query9))
}

/**
For n in N Union
  Sng((nation:= n.n_name, opertions := For p in Query3 Union
    Sng((customers := For c in p.customers Union
      If (c.c_nationkey = n.nationkey)
      Then Sng((c_name := c.c_name)),
    Sng((suppliers := For s in p.suppliers Union
      If (s.s_nationkey != n.nation_key)
      Then Sng((s_name := s.s_name))))))))
**/
object TPCHNested10 extends TPCHBase {
  val name = "TPCHNested10"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x =>
      List("C", "O", "L", "P", "PS", "S", "N").contains(x._1)).values.toList.mkString("")}"

  val (q3r, cor) = varset(TPCHQuery3Full.name, "co",
    TPCHQuery3Full.program(TPCHQuery3Full.name).varRef.asInstanceOf[BagExpr])

  val customers = BagProject(cor, "customers")
  val c2r = TupleVarRef(Symbol.fresh(), customers.tp.tp)

  val suppliers = BagProject(cor, "suppliers")
  val s2r = TupleVarRef(Symbol.fresh(), suppliers.tp.tp)

  val query10 = ForeachUnion(nr, relN,
    Singleton(Tuple("nation" -> nr("n_name"), "operations" -> ForeachUnion(cor, q3r,
      Singleton(Tuple("customers" -> ForeachUnion(c2r, customers,
        IfThenElse(Cmp(OpEq, nr("n_nationkey"), c2r("c_nationkey")),
          Singleton(Tuple("c_name" -> c2r("c_name"))))),
       "suppliers" -> ForeachUnion(s2r, suppliers,
        IfThenElse(Not(Cmp(OpEq, s2r("s_nationkey"), nr("n_nationkey"))),
          Singleton(Tuple("s_name" -> s2r("s_name")))))
      ))))))

  val program = Program(Assignment(name, query10))
}
