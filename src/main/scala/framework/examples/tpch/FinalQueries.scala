package framework.examples.tpch

import framework.core._

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

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(lr, relL,
                     ForeachUnion(pr, relP,
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljpr, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lpr, ljpr,
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty"))))))))))))

  val program = Program(Assignment(ljpr.name, query1_ljp), Assignment(name, query1))
}

object Query1Full extends TPCHBase {
  val name = "Query1"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lr, relL,
                        IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                       ForeachUnion(pr, relP,
                         IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                          Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

  val program = Program(Assignment(name, query1))
}

object Query1BU extends TPCHBase {
  val name = "Query1"
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_c_orders_1", 
    s"${name}__D_1_c_orders_1_o_parts_1")

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val lquery = ForeachUnion(lr, relL, 
    ForeachUnion(pr, relP, 
      IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
        Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), "p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))
  
  val (parts, partRef) = varset("parts", "part", lquery)
  val oquery = 
    ForeachUnion(or, relO,
      Singleton(Tuple("o_custkey" -> or("o_custkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
        ForeachUnion(partRef, parts,
          IfThenElse(Cmp(OpEq, or("o_orderkey"), partRef("l_orderkey")),
            Singleton(Tuple("p_name" -> partRef("p_name"), "l_qty" -> partRef("l_qty"))))))))
  
  val (orders, orderRef) = varset("orders", "order", oquery)
  val query = 
    ForeachUnion(cr, relC,
      Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> 
        ForeachUnion(orderRef, orders,
          IfThenElse(Cmp(OpEq, cr("c_custkey"), orderRef("o_custkey")),
            Singleton(Tuple("o_orderdate" -> orderRef("o_orderdate"), 
              "o_parts" -> orderRef("o_parts"))))))))

  val program = Program(Assignment(parts.name, lquery), Assignment(orders.name, oquery), Assignment(name, query))
}


object Query1Filter extends TPCHBase {
  val name = "Query1Filter"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(lr, relL,
                     ForeachUnion(pr, relP,
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljpr, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(cr, relC,
                IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")), 
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                  IfThenElse(And(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Cmp(OpGe, Const(150000000, IntType), or("o_orderkey"))), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lpr, ljpr,
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty")))))))))))))

  val program = Program(Assignment(ljpr.name, query1_ljp), Assignment(name, query1))
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
  val name = "Query2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(Query1.name, "c2",
    Query1.program(Query1.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query2 =
    ForeachUnion(cor, q1r,
      Singleton(Tuple("c_name" -> cor("c_name"), "totals" ->
        ReduceByKey(
          ForeachUnion(co2r, orders,
            ForeachUnion(co3r, parts,
              Singleton(Tuple("orderdate" -> co2r("o_orderdate"),
                "pname" -> co3r("p_name"), "qty" -> co3r("l_qty"))))),
          List("orderdate", "pname"),
          List("qty")
        ))))

  val program =
    Query1.program.asInstanceOf[Program] ++
      Program(Assignment(name, query2))
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
  val name = "Query3"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(Query1.name, "c2",
    Query1.program(Query1.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query3 =
    ReduceByKey(
      ForeachUnion(cor, q1r,
        ForeachUnion(co2r, orders,
          ForeachUnion(co3r, parts,
            ForeachUnion(pr, relP,
              IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                Singleton(Tuple("c_name" -> cor("c_name"), "p_name" -> pr("p_name"), 
                                "total" ->  co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))))),
      List("c_name", "p_name"),
      List("total")
    )

  val program =
    Query1.program.asInstanceOf[Program] ++
      Program(Assignment(name, query3))
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
  val name = "Query4"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(Test2.name, "c2",
    Test2.program(Test2.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ForeachUnion(cor, q1r,
      Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
        ForeachUnion(co2r, orders,
          Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
            ReduceByKey(
              ForeachUnion(co3r, parts,
                ForeachUnion(pr, relP,
                  IfThenElse(Cmp(OpEq, pr("p_partkey"), co3r("l_partkey")),
                    Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                      co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))),
              List("p_name"),
              List("total")
            )))))))

  val program = Program(Assignment(name, query4))
}

/** 
This is just an extended version of Query 1 
which is passed to Query4Filter* to 
make the application of filters easier.
**/
object Query1Extended extends TPCHBase {
  val name = "Query1Extended"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  override def indexedDict: List[String] =
    List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")

  val query1_ljp = ForeachUnion(lr, relL,
                     ForeachUnion(pr, relP,
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity"))))))
  
  val (ljpr, lpr) = varset("ljp", "lp", query1_ljp)

  val query1 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_custkey" -> cr("c_custkey"), "c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(or, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lpr, ljpr,
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty"))))))))))))

  val program = Program(Assignment(ljpr.name, query1_ljp), Assignment(name, query1))
}

object Query4Filter1 extends TPCHBase {
  val name = "Query4Filter1"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(Query1Extended.name, "c2",
    Query1Extended.program(Query1Extended.name).varRef.asInstanceOf[BagExpr])

  val orders = BagProject(cor, "c_orders")
  val co2r = TupleVarRef("o2", orders.tp.tp)

  val parts = BagProject(co2r, "o_parts")
  val co3r = TupleVarRef("p2", parts.tp.tp)

  val query4 =
    ForeachUnion(cor, q1r,
      IfThenElse(Cmp(OpGe, Const(1500000, IntType), cr("c_custkey")),
        Singleton(Tuple("c_name" -> cor("c_name"), "c_orders" ->
          ForeachUnion(co2r, orders,
            Singleton(Tuple("o_orderdate" -> co2r("o_orderdate"), "o_parts" ->
              ReduceByKey(
                ForeachUnion(co3r, parts,
                  ForeachUnion(pr, relP,
                    IfThenElse(Cmp(OpEq, pr("p_name"), co3r("p_name")),
                        Singleton(Tuple("p_name" -> pr("p_name"), "total" -> 
                          co3r("l_qty").asNumeric * pr("p_retailprice").asNumeric))))),
                List("p_name"),
                List("total")
              ))))))))

  val program =
    Query1Extended.program.asInstanceOf[Program] ++
      Program(Assignment(name, query4))
}

object Query4Filter2 extends TPCHBase {
  val name = "Query4Filter2"

  def inputs(tmap: Map[String, String]): String =
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val (q1r, cor) = varset(Query1Extended.name, "c2",
    Query1Extended.program(Query1Extended.name).varRef.asInstanceOf[BagExpr])

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
              ReduceByKey(
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
  val name = "Query5"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"""
    // List(s"${name}__D_1", s"${name}__D_2c_orders_1", s"${name}__D_2c_orders_2o_parts_1")
  override def indexedDict: List[String] = List(s"${name}__D_1", s"${name}__D_1_customers2_1")

  val custs = 
      ForeachUnion(or, relO,
          ForeachUnion(cr, relC,
            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
              Singleton(Tuple("c_orderkey" -> or("o_orderkey"),
                 "c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"))))))
  val (customers, customersRef) = varset("customers", "co", custs)

  val custsKeyed = ForeachUnion(customersRef, customers, 
    ForeachUnion(lr, relL,
      IfThenElse(Cmp(OpEq, lr("l_orderkey"), customersRef("c_orderkey")),
        projectTuple(customersRef, "c_suppkey" -> lr("l_suppkey"), List("c_orderkey")))))
  val (csuppkeys, csuppRef) = varset("csupps", "cs", custsKeyed)
                       
  val query5 = ForeachUnion(sr, relS,
                Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"), "customers2" -> 
                  ForeachUnion(csuppRef, csuppkeys,
                    IfThenElse(Cmp(OpEq, csuppRef("c_suppkey"), sr("s_suppkey")),
                      projectBaseTuple(csuppRef, List("c_suppkey"))))))) 

  val program = Program(Assignment(customers.name, custs), 
    Assignment(csuppkeys.name, custsKeyed), Assignment(name, query5))
}

/**

This is Query6 without specifying Let's, which is more 
optimal for the version of the query that does 
not apply the optimization.

For c in C Union
  Sng((c_name := c.c_name, suppliers := For co in Query2 Union
    For co2 in co.customers2 Union
      If (co2.c_name2 = c.c_name)
      Then Sng((s_name := co.s_name))))
**/
object Query6Full extends TPCHBase {
  val name = "Query6Full"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"""
 
  val (q2r, cor) = varset(Query5.name, "co",
    Query5.program(Query5.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)

  val query6 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  ForeachUnion(cor, q2r,
                    ForeachUnion(co2r, cust,
                      IfThenElse(Cmp(OpEq, co2r("c_name"), cr("c_name")),
                        Singleton(Tuple("s_name" -> cor("s_name")))))))))

  val program = Program(Assignment(name, query6))
}

object Query6GBK extends TPCHBase {
  val name = "Query6GBK"

  def inputs(tmap: Map[String, String]): String = 
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"""
 
  val (q2r, cor) = varset(Query5.name, "co",
    Query5.program(Query5.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)

  val cflat = ForeachUnion(cor, q2r,
                    ForeachUnion(co2r, cust,
                      Singleton(Tuple("c_name" -> co2r("c_name"), "s_name" -> cor("s_name")))))
  val (cfr, c2r) = varset("cflat", "c2", cflat)
  val query6 = GroupByKey(cfr, List("c_name"), List("s_name"))

  val program = Program(Assignment(cfr.name, cflat), Assignment(name, query6))
}

/**

This is Query 6 with the Let, which helps with 
join order in the version with the optimization.

cflat := For co in Query5 Union
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
    s"""val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"""
 
  val (q5r, cor) = varset(Query5.name, "co",
    Query5.program(Query5.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)
  
  val flat = ForeachUnion(cor, q5r,
              ForeachUnion(co2r, cust,
                Singleton(Tuple("c_name" -> co2r("c_name"), "s_name" -> cor("s_name"), 
                  "s_nationkey" -> cor("s_nationkey")))))
  val (cflatr, cfr) = varset("cflat", "cf", flat.asInstanceOf[BagExpr])

  val query6 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"), "suppliers" -> 
                  ForeachUnion(cfr, cflatr,
                    IfThenElse(Cmp(OpEq, cfr("c_name"), cr("c_name")),
                      projectBaseTuple(cfr, List("s_suppkey")))))))

  val program = Program(Assignment(cflatr.name, flat), Assignment(name, query6))
}

/**
cflat := For co in Query5 Union
  For co2 in co.customers2 Union
    Sng((c_name := co2.c_name2, s_name := co.s_name))

For c in C Union
  Sng((c_name := c.c_name, suppliers := Total(For cf in cflat Union
    If (cf.c_name = c.c_name)
    Then Sng((s_name := cf.s_name)))))
**/
object Query7 extends TPCHBase {
  val name = "Query7"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q5r, cor) = varset(Query5.name, "co",
    Query5.program(Query5.name).varRef.asInstanceOf[BagExpr])

  val cust = BagProject(cor, "customers2")
  val co2r = TupleVarRef("co2", cust.tp.tp)
  
  val flat = ForeachUnion(cor, q5r,
              ForeachUnion(co2r, cust,
                Singleton(Tuple("c_name" -> co2r("c_name2"), "s_name" -> cor("s_name")))))
  val (cflatr, cfr) = varset("cflat", "cf", flat.asInstanceOf[BagExpr])
  val query7 = ForeachUnion(cr, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "suppliers" -> 
                  Count(ForeachUnion(cfr, cflatr,
                    IfThenElse(Cmp(OpEq, cfr("c_name"), cr("c_name")),
                      Singleton(Tuple("s_name" -> cfr("s_name")))))))))

  val program =
    Query5.program.asInstanceOf[Program] ++
      Program(Assignment(cflatr.name, flat), Assignment(name, query7))
}
