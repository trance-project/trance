package shredding.queries.tpch

import shredding.core._
import shredding.nrc.LinearizedNRC

trait Queries extends LinearizedNRC {

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
  
  val relC = BagVarRef(VarDef("C", TpchSchema.customertype))
  val c = VarDef("c", TpchSchema.customertype.tp)
  val cr = TupleVarRef(c) 

  val relO = BagVarRef(VarDef("O", TpchSchema.orderstype))
  val o = VarDef("o", TpchSchema.orderstype.tp)
  val or = TupleVarRef(o)

  val relL = BagVarRef(VarDef("L", TpchSchema.lineittype))
  val l = VarDef("l", TpchSchema.lineittype.tp)
  val lr = TupleVarRef(l)

  val relP = BagVarRef(VarDef("P", TpchSchema.parttype))
  val p = VarDef("p", TpchSchema.parttype.tp)
  val pr = TupleVarRef(p)

  val query1 = ForeachUnion(c, relC, 
            Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
              IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL, 
                  ForeachUnion(p, relP, IfThenElse(
                    And(Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")), Cmp(OpEq, lr("l_partkey"), pr("p_partkey"))), 
                      Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity")))))))))))))

  val q1 = VarDef("Q1", TupleType("c_name" -> StringType, "c_orders" -> 
                          BagType(TupleType("o_orderdate" -> StringType, "o_parts" -> 
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> IntType)))))) 
  val q1r = TupleVarRef(q1)
  val cq1 = VarDef("corders", TupleType("o_orderdate" -> StringType, "o_parts" ->
                            BagType(TupleType("p_name" -> StringType, "l_qty" -> IntType))))
  val cq1r = TupleVarRef(cq1)

  val pq1 = VarDef("oparts", TupleType("p_name" -> StringType, "l_qty" -> IntType))
  val pq1r = TupleVarRef(pq1)

  // query 4 
  
  // Query 2

  val relS = BagVarRef(VarDef("S", TpchSchema.suppliertype))
  val s = VarDef("s", TpchSchema.suppliertype.tp)
  val sr = TupleVarRef(s)
  val query2 = ForeachUnion(s, relS, 
            Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> ForeachUnion(l, relL, 
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")), 
                ForeachUnion(o, relO, 
                  IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                    ForeachUnion(c, relC, 
                      IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                        Singleton(Tuple("c_name2" -> cr("c_name"))))))))))))


  val relPS = BagVarRef(VarDef("PS", TpchSchema.partsupptype))
  val ps = VarDef("ps", TpchSchema.partsupptype.tp)
  val psr = TupleVarRef(ps) 

  val query3 = ForeachUnion(p, relP, 
                Singleton(Tuple("p_name" -> pr("p_name"), "suppliers" -> ForeachUnion(ps, relPS, 
                  IfThenElse(Cmp(OpEq, psr("ps_partkey"), pr("p_partkey")),
                    ForeachUnion(s, relS, 
                      IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")), 
                        Singleton(Tuple("s_name" -> sr("s_name"))))))),
                  "customers" -> ForeachUnion(l, relL, 
                    IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
                      ForeachUnion(o, relO, 
                        IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                          ForeachUnion(c, relC, 
                            IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                              Singleton(Tuple("c_name" -> cr("c_name"))))))))))))
}
