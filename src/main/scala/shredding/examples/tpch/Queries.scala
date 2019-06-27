package shredding.examples.tpch

import shredding.core._
import shredding.nrc.LinearizedNRC

object TPCHQueries { 
  val nrc = new LinearizedNRC {}
  import nrc._
  
  /**
    * var Q1 =
    * for (c <- Customer) yield (c.name,
    *   for (o <- Orders
    * if o.custkey == c.custkey)
    * yield (o.orderdate,
    * for (l <- Lineitem; p <- Part
    * if l.orderkey == o.orderkey && l.partkey == p.partkey)
    * yield (p.name, l.qty) ))
    */
  
  val relC = BagVarRef(VarDef("C", TPCHSchema.customertype))
  val c = VarDef("c", TPCHSchema.customertype.tp)
  val cr = TupleVarRef(c) 

  val relO = BagVarRef(VarDef("O", TPCHSchema.orderstype))
  val o = VarDef("o", TPCHSchema.orderstype.tp)
  val or = TupleVarRef(o)

  val relL = BagVarRef(VarDef("L", TPCHSchema.lineittype))
  val l = VarDef("l", TPCHSchema.lineittype.tp)
  val lr = TupleVarRef(l)

  val relP = BagVarRef(VarDef("P", TPCHSchema.parttype))
  val p = VarDef("p", TPCHSchema.parttype.tp)
  val pr = TupleVarRef(p)

  val q1name = "Query1"
  val q1data = s"""
    |val C = TPCHLoader.loadCustomer[Customer].toList
    |val O = TPCHLoader.loadOrders[Orders].toList
    |val L = TPCHLoader.loadLineitem[Lineitem].toList
    |val P = TPCHLoader.loadPart[Part].toList""".stripMargin
  
  val sq1data = s"""
    |val C__F = 1
    |val C__D = (List((C__F, TPCHLoader.loadCustomer[Customer].toList)), ())
    |val O__F = 2
    |val O__D = (List((O__F, TPCHLoader.loadOrders[Orders].toList)), ())
    |val L__F = 3
    |val L__D = (List((L__F, TPCHLoader.loadLineitem[Lineitem].toList)), ())
    |val P__F = 4
    |val P__D = (List((P__F, TPCHLoader.loadPart[Part].toList)), ())""".stripMargin
 
  val query1 = ForeachUnion(c, relC, 
            Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
              IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL, 
                  IfThenElse(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
                    ForeachUnion(p, relP, IfThenElse(
                      Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                        Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

  val q1type = TupleType("c_name" -> StringType, "c_orders" ->
                          BagType(TupleType("o_orderdate" -> StringType, "o_parts" ->
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType)))))

  val Q1 = VarDef("Q1", query1.tp) 
  val q1 = VarDef("q1", q1type)
  val q1r = TupleVarRef(q1)
  val cq1 = VarDef("corders", TupleType("o_orderdate" -> StringType, "o_parts" ->
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> DoubleType))))
  val cq1r = TupleVarRef(cq1)

  val pq1 = VarDef("oparts", TupleType("p_name" -> StringType, "l_qty" -> DoubleType))
  val pq2 = VarDef("oparts2", TupleType("p_name" -> StringType, "l_qty" -> DoubleType))

  val pq1r = TupleVarRef(pq1)

  val qt = VarDef("qt", IntType)

  /** 
    * need to add primitive support for month extraction
    *
    * var Q4 = (
    *   for ((c_name, c_orders) <- Q1; 
    *        (o_orderdate, o_parts) <- c_orders; 
    *        (p_name, l_qty) <- o_parts)
    *   yield ((c_name,p_name,getMonth(o_orderdate)),l_qty) ) 
    *    .reduceByKey(_ + _)
    */
  val q4name = "Query4"

  val sq4cclass = (r1: String, r2: String) => s"""
    |case class Input_Q1_Dict2(o_parts: (List[$r2], Unit))
    |case class Input_Q1_Dict1(c_orders: (List[$r1], Input_Q1_Dict2))""".stripMargin
  val sq4data = (v: String) => s"""
    |val Q1__F = $v._1.head.lbl
    |val Q1__D = ($v._2, Input_Q1_Dict1(($v._4, Input_Q1_Dict2(($v._6, Unit)))))""".stripMargin

  val query4 = //Sequence(List(Named(Q1, query1),
                ForeachUnion(q1, BagVarRef(Q1), 
                  ForeachUnion(cq1, BagProject(q1r, "c_orders"), 
                    ForeachUnion(pq1, BagProject(cq1r, "o_parts"), 
                      Singleton(Tuple("c_name" -> q1r("c_name"), "p_name" -> pq1r("p_name"), "month" -> cq1r("o_orderdate"), 
                        "t_qty" -> Total(ForeachUnion(pq2, BagProject(cq1r, "o_parts"), 
                                  IfThenElse(Cmp(OpEq, TupleVarRef(pq2)("p_name"), pq1r("p_name")),
                                    WeightedSingleton(Tuple("l_qty" -> pq1r("l_qty")), 
                                      TupleVarRef(pq2)("l_qty").asInstanceOf[PrimitiveExpr])))))))))//))   
  // Query 2

  val relS = BagVarRef(VarDef("S", TPCHSchema.suppliertype))
  val s = VarDef("s", TPCHSchema.suppliertype.tp)
  val sr = TupleVarRef(s)
  val query2 = ForeachUnion(s, relS, 
            Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> ForeachUnion(l, relL, 
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")), 
                ForeachUnion(o, relO, 
                  IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                    ForeachUnion(c, relC, 
                      IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                        Singleton(Tuple("c_name2" -> cr("c_name"))))))))))))


  val relPS = BagVarRef(VarDef("PS", TPCHSchema.partsupptype))
  val ps = VarDef("ps", TPCHSchema.partsupptype.tp)
  val psr = TupleVarRef(ps) 

  // Query 3
  val q3data = s"""
    |val C = TPCHLoader.loadCustomer[Customer].toList
    |val O = TPCHLoader.loadOrders[Orders].toList
    |val L = TPCHLoader.loadLineitem[Lineitem].toList
    |val P = TPCHLoader.loadPart[Part].toList
    |val PS = TPCHLoader.loadPartSupp[PartSupp].toList
    |val S = TPCHLoader.loadSupplier[Supplier].toList""".stripMargin

  val q3name = "Query3"
  val query3 = ForeachUnion(p, relP, 
                Singleton(Tuple("p_name" -> pr("p_name"), "suppliers" -> ForeachUnion(ps, relPS, 
                  IfThenElse(Cmp(OpEq, psr("ps_partkey"), pr("p_partkey")),
                    ForeachUnion(s, relS, 
                      IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")), 
                        Singleton(Tuple("s_name" -> sr("s_name"), "s_nationkey" -> sr("s_nationkey"))))))),
                  "customers" -> ForeachUnion(l, relL, 
                    IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                      ForeachUnion(o, relO, 
                        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                          ForeachUnion(c, relC, 
                            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                              Singleton(Tuple("c_name" -> cr("c_name"), "c_nationkey" -> cr("c_nationkey"))))))))))))
  
  // Query 5

  val Q3 = VarDef("Q3", query3.tp)
  val q3 = VarDef("q3", query3.tp.asInstanceOf[BagType].tp)
  val c5 = VarDef("c", query3.tp.asInstanceOf[BagType].tp.attrTps("customers").asInstanceOf[BagType].tp)
  val s5 = VarDef("s", query3.tp.asInstanceOf[BagType].tp.attrTps("suppliers").asInstanceOf[BagType].tp)

  val rq3 = TupleVarRef(q3)
  val rc5 = TupleVarRef(c5)
  val rs5 = TupleVarRef(s5)
  
  val query5 = ForeachUnion(q3, BagVarRef(Q3), 
                Singleton(Tuple("p_name" -> rq3("p_name"), "cnt" -> 
                  Total(ForeachUnion(c5, BagProject(rq3, "customers"),
                    IfThenElse(Cmp(OpEq, 
                      Total(ForeachUnion(s5, BagProject(rq3, "suppliers"), 
                              IfThenElse(Cmp(OpEq, rc5("c_nationkey"), rs5("s_nationkey")), 
                                Singleton(Tuple("flag" -> Const(true, BoolType)))))),
                      Const(0, IntType)),
                      Singleton(Tuple("p_name" -> rq3("p_name")))))))))
}
