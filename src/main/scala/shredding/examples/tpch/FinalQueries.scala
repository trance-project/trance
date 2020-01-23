package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.LinearizedNRC

/**
  * // this is query1_ljp
  * Let ljp = For l in L Union
  *            For p in P Union
  *              If (l.l_partkey = p.p_partkey) // skew join
  *              Then Sng((l_orderkey := l_orderkey, p_name := p.p_name, l_qty := l.l_quantity))
  *
  * // this is query1
  * For c in C Union
  *  Sng((c_name := c.c_name, c_orders := 
  *    For o in O Union
  *      If (o.o_custkey = c.c_custkey)
  *      Then Sng((o_orderdate := o.o_orderdate, o_parts := 
  *        For l in ljp Union
  *           If (l.l_orderkey = o.o_orderkey)
  *           Then Sng((p_name := p.p_name, l_qty := l.l_quantity))))))
  */

object Query1 extends TPCHBase {
  val name = "Query1"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(l, relL,
                     ForeachUnion(p, relP, 
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljp, lp, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(c, relC, 
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lp, BagVarRef(ljp),
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty"))))))))))))
  val query = Sequence(List(Named(ljp, query1_ljp), query1))
}


object Query1Filter extends TPCHBase {
  val name = "Query1Filter"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(l, relL,
                     ForeachUnion(p, relP, 
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljp, lp, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(c, relC, 
                IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")), 
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
                  IfThenElse(And(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Cmp(OpGe, Const(150000000, IntType), or("o_orderkey"))), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lp, BagVarRef(ljp),
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty")))))))))))))
  val query = Sequence(List(Named(ljp, query1_ljp), query1))
}

/**
For c2 in Query1 Union
  Sng((c_name := c2.c_name, totals := (
    For o2 in c2.c_orders Union
      For p2 in o2.o_parts Union
        Sng((orderdate := o2.o_orderdate, pname := p2.p_name, qty := p2.l_qty))
    ).groupBy+((orderdate := x1.orderdate, pname := x1.pname)), x1.qty)))
**/
object Query2 extends TPCHBase {
  // val name = "TPCHNested1"
  val name = "Query2"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1.name, "c2", TPCHQuery1Full.query.asInstanceOf[BagExpr])
  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query = 
    Let(q1, Query1.query1.asInstanceOf[BagExpr],
    ForeachUnion(co, BagVarRef(q1),
                Singleton(Tuple("c_name" -> cor("c_name"), "totals" ->
                  GroupBy(
                    ForeachUnion(co2, orders,
                      ForeachUnion(co3, parts,
                        Singleton(Tuple("orderdate" -> co2r("o_orderdate"),
                          "pname" -> co3r("p_name"), "qty" -> co3r("l_qty"))))),
                 List("orderdate", "pname"),
                 List("qty"),
                 DoubleType)))))
}

/**
(For c2 in Query1 Union
  For o2 in c2.c_orders Union
    For p2 in o2.o_parts Union
      For p in P Union
        If (p.p_name = p2.p_name)
        Then Sng((c_name := c2.c_name, p_name := p.p_name, total := p2.l_qty*p.p_retailprice))
  ).groupBy+((c_name := x1.c_name, p_name := x1.p_name), x1.total)
**/
object Query3 extends TPCHBase {
  // val name = "TPCHNested2Unopt"
  val name = "Query3"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1.name, "c2", TPCHQuery1Full.query.asInstanceOf[BagExpr])
  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query = 
    GroupBy(
      ForeachUnion(co, BagVarRef(q1),
        ForeachUnion(co2, orders, 
          ForeachUnion(co3, parts, 
            ForeachUnion(p, relP, 
              IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                                "total" -> PrimitiveOp(Multiply, co3r("l_qty"), pr("p_retailprice"))))))))),
      List("c_name", "p_name"),
      List("total"),
      DoubleType)
              
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
**/
object Query4 extends TPCHBase {
  // val name = "TPCHNested4"
  val name = "Query4"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1.name, "c2", TPCHQuery1Full.query.asInstanceOf[BagExpr])
  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query = 
    ForeachUnion(co, BagVarRef(q1),
      Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
        ForeachUnion(co2, orders, 
          Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
            GroupBy(
              ForeachUnion(co3, parts, 
                ForeachUnion(p, relP, 
                  IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                      PrimitiveOp(Multiply, co3r("l_qty"), pr("p_retailprice"))))))),
      List("p_name"),
      List("total"),
      DoubleType)))))))          
}

/** 
This is just an extended version of Query 1 
which is passed to Query4Filter* to 
make the application of filters easier.
**/
object Query1Extended extends TPCHBase {
  // val name = "Query1WK"
  val name = "Query1Extended"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(l, relL,
                     ForeachUnion(p, relP, 
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljp, lp, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(c, relC, 
                Singleton(Tuple("c_custkey" -> cr("c_custkey"), "c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lp, BagVarRef(ljp),
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty"))))))))))))
  val query = Sequence(List(Named(ljp, query1_ljp), query1))
}

object Query4Filter1 extends TPCHBase {
  // val name = "TPCHNested4Filter"
  val name = "Query4Filter1"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(TPCHQuery1WK.name, "c2", Query1Extended.query1.asInstanceOf[BagExpr])
  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query = 
    ForeachUnion(co, BagVarRef(q1),
      IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")),
        Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
          ForeachUnion(co2, orders, 
            Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
              GroupBy(
                ForeachUnion(co3, parts, 
                  ForeachUnion(p, relP, 
                    IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                          PrimitiveOp(Multiply, co3r("l_qty"), pr("p_retailprice"))))))),
        List("p_name"),
        List("total"),
        DoubleType))))))))          
}

object Query4Filter2 extends TPCHBase {
  // val name = "TPCHNested4Filter"
  val name = "Query4Filter2"
  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1, co, cor) = varset(Query1Extended.name, "c2", Query1Extended.query1.asInstanceOf[BagExpr])
  val orders = BagProject(cor, "c_orders")
  val co2 = VarDef("o2", orders.tp.tp)
  val co2r = TupleVarRef(co2)

  val parts = BagProject(co2r, "o_parts")
  val co3 = VarDef("p2", parts.tp.tp)
  val co3r = TupleVarRef(co3)

  val query =
    Let(q1, Query1Extended.query1.asInstanceOf[BagExpr], 
    ForeachUnion(co, BagVarRef(q1),
      IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")),
        Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
          ForeachUnion(co2, orders, 
            IfThenElse(Cmp(OpGe, Const(150000000, IntType), or("o_orderkey")),
            Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
              GroupBy(
                ForeachUnion(co3, parts, 
                  ForeachUnion(p, relP, 
                    IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                          PrimitiveOp(Multiply, co3r("l_qty"), pr("p_retailprice"))))))),
        List("p_name"),
        List("total"),
        DoubleType))))))))))          
}

/**
Let resultInner := For c in C Union
  For o in O Union
    If (c.c_custkey = o.o_custkey) // skew join
    Then Sng((o_orderkey := o.o_orderkey, c_name := c.c_name))

For s in S Union
  Sng((s_name := s.s_name, customers2 := For l in L Union
    If (s.s_suppkey = l.l_suppkey)
    Then For co in resultInner Union
      If (co.o_orderkey = l.l_orderkey)
      Then Sng((c_name2 := co.c_name))))
**/

object Query5 extends TPCHBase {

  // val name = "Query2"
  val name = "Query5"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_2customers2_1")
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val resultInner = 
    //ForeachUnion(l, relL, 
      ForeachUnion(o, relO, 
        //IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
          ForeachUnion(c, relC, // skew join
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "c_name" -> cr("c_name"))))))//))
  val (ri, co, cor) = varset("resultInner", "co", resultInner)                     
                       
  val result = ForeachUnion(s, relS,
                Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> 
                  ForeachUnion(l, relL, 
                    IfThenElse(Cmp(OpEq, lr("l_suppkey"), sr("s_suppkey")),
                      ForeachUnion(co, BagVarRef(ri), 
                        IfThenElse(Cmp(OpEq, lr("l_orderkey"), cor("o_orderkey")),
                          Singleton(Tuple("c_name2" -> cor("c_name")))))))))) 

  val query = Sequence(List(Named(ri, resultInner), result))
}

/**

This is Query6 without specifying Let's, which is more 
optimal for the version of the query that does 
not apply the optimization.

For c in C Union
  Sng((c_name := c.c_name, suppliers := For co in Query2Full Union
    For co2 in co.customers2 Union
      If (co2.c_name2 = c.c_name)
      Then Sng((s_name := co.s_name))))
**/
object Query6Full extends TPCHBase {
  val name = "Query6Full"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2, co, cor) = varset(TPCHQuery2.name, "co", TPCHQuery2Full.query.asInstanceOf[BagExpr])
  val cust = BagProject(cor, "customers2")
  val co2 = VarDef("co2", cust.tp.tp)
  val co2r = TupleVarRef(co2)

  val query = ForeachUnion(c, relC, 
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  ForeachUnion(co, BagVarRef(q2),
                    ForeachUnion(co2, cust,
                      IfThenElse(Cmp(OpEq, co2r("c_name2"), cr("c_name")),
                        Singleton(Tuple("s_name" -> cor("s_name"))))))))) 

}

/**

This is Query 6 with the Let, which helps with 
join order in the version with the optimization.

cflat := For co in Query2 Union
 For co2 in co.customers2 Union
   Sng((c_name := co2.c_name2, s_name := co.s_name))
For c in C Union
 Sng((c_name := c.c_name, suppliers := For cf in cflat Union
   If (cf.c_name = c.c_name)
   Then Sng((s_name := cf.s_name))))
**/
object Query6 extends TPCHBase {
  val name = "Query6"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2, co, cor) = varset(TPCHQuery2.name, "co", TPCHQuery2Full.query.asInstanceOf[BagExpr])
  val cust = BagProject(cor, "customers2")
  val co2 = VarDef("co2", cust.tp.tp)
  val co2r = TupleVarRef(co2)
  
  val flat = ForeachUnion(co, BagVarRef(q2),
              ForeachUnion(co2, cust,
                Singleton(Tuple("c_name" -> co2r("c_name2"), "s_name" -> cor("s_name")))))
  val (cflat, cf, cfr) = varset("cflat", "cf", flat.asInstanceOf[BagExpr])
  val query = Sequence(List(Named(cflat, flat),
    ForeachUnion(c, relC, 
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  ForeachUnion(cf, BagVarRef(cflat),
                    IfThenElse(Cmp(OpEq, cfr("c_name"), cr("c_name")),
                      Singleton(Tuple("s_name" -> cfr("s_name"))))))))))

}

/**
cflat := For co in Query2 Union
  For co2 in co.customers2 Union
    Sng((c_name := co2.c_name2, s_name := co.s_name))

For c in C Union
  Sng((c_name := c.c_name, suppliers := Total(For cf in cflat Union
    If (cf.c_name = c.c_name)
    Then Sng((s_name := cf.s_name)))))
**/
object Query7 extends TPCHBase {
  // val name = "TPCHNested8"
  val name = "Query7"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2, co, cor) = varset(TPCHQuery2.name, "co", TPCHQuery2Full.query.asInstanceOf[BagExpr])
  val cust = BagProject(cor, "customers2")
  val co2 = VarDef("co2", cust.tp.tp)
  val co2r = TupleVarRef(co2)
  
  val flat = ForeachUnion(co, BagVarRef(q2),
              ForeachUnion(co2, cust,
                Singleton(Tuple("c_name" -> co2r("c_name2"), "s_name" -> cor("s_name")))))
  val (cflat, cf, cfr) = varset("cflat", "cf", flat.asInstanceOf[BagExpr])
  val query = Sequence(List(Named(cflat, flat),
    ForeachUnion(c, relC, 
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  Total(ForeachUnion(cf, BagVarRef(cflat),
                    IfThenElse(Cmp(OpEq, cfr("c_name"), cr("c_name")),
                      Singleton(Tuple("s_name" -> cfr("s_name")))))))))))

}
