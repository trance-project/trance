package shredding.examples.tpch

import shredding.core._
import shredding.examples.Query
import shredding.nrc.LinearizedNRC
import shredding.wmcc._

trait TPCHBase extends Query {

  def varset(n1: String, n2: String, e: BagExpr): (VarDef, VarDef, TupleVarRef) = {
    val vd = VarDef(n2, e.tp.tp)
    (VarDef(n1, e.tp), vd, TupleVarRef(vd))
  }

  // append other type maps
  def inputTypes(shred: Boolean = false): Map[Type, String] = 
    if (shred) TPCHSchema.tpchInputs.map(f => translate(f._1) -> f._2) ++ TPCHSchema.tpchShredInputs
    else TPCHSchema.tpchInputs.map(f => translate(f._1) -> f._2)

  def headerTypes(shred: Boolean = false): List[String] = inputTypes(shred).map(f => f._2).toList

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

  val relS = BagVarRef(VarDef("S", TPCHSchema.suppliertype))
  val s = VarDef("s", TPCHSchema.suppliertype.tp)
  val sr = TupleVarRef(s)
  
  val relPS = BagVarRef(VarDef("PS", TPCHSchema.partsupptype))
  val ps = VarDef("ps", TPCHSchema.partsupptype.tp)
  val psr = TupleVarRef(ps)  

}
  
object TPCHQuery1Full extends TPCHBase {

  val name = "Query1Full"

  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"
 
  val query = ForeachUnion(c, relC,
    Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO,
      IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
        Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> ForeachUnion(l, relL,
          IfThenElse(
            Cmp(OpEq, lr("l_orderkey"), or("o_orderkey")),
            ForeachUnion(p, relP, IfThenElse(
              Cmp(OpEq, lr("l_partkey"), pr("p_partkey")),
              Singleton(Tuple("p_name" -> pr("p_name"), "l_qty" -> lr("l_quantity"))))))))))))))

}

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

object TPCHQuery1 extends TPCHBase {
  val name = "Query1"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

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


object TPCHQuery3Full extends TPCHBase{
  val name = "Query3Full"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "P", "PS", "S").contains(x._1)).values.toList.mkString("")}"

  val query = ForeachUnion(p, relP,
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
}

object TPCHQuery2Full extends TPCHBase {

  val name = "Query2Full"
  override def indexedDict: List[String] = List("Query2Full__D_1", "Query2Full__D_2customers2_1")
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val query = ForeachUnion(s, relS,
            Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> ForeachUnion(l, relL,
              IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")),
                ForeachUnion(o, relO,
                  IfThenElse(Cmp(OpEq, or("o_orderkey"), lr("l_orderkey")),
                    ForeachUnion(c, relC,
                      IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                        Singleton(Tuple("c_name2" -> cr("c_name"))))))))))))
  /**val query = ForeachUnion(s, relS,
                Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> 
                    ForeachUnion(c, relC,
                      ForeachUnion(o, relO,
                        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                          ForeachUnion(l, relL,
                            IfThenElse(And(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")),
                                           Cmp(OpEq, or("o_orderkey"), lr("l_orderkey"))),
                              Singleton(Tuple("c_name2" -> cr("c_name")))))))))))**/

}

object TPCHQuery2 extends TPCHBase {

  val name = "Query2"
  override def indexedDict: List[String] = List("Query2Full__D_1", "Query2Full__D_2customers2_1")
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val resultInner = ForeachUnion(o, relO, 
                      ForeachUnion(c, relC, // skew join
                        IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                          Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "c_name" -> cr("c_name"))))))
  val (ri, co, cor) = varset("resultInner", "co", resultInner)                     
                       
  val result = ForeachUnion(s, relS,
            Singleton(Tuple("s_name" -> sr("s_name"), "customers2" -> 
                ForeachUnion(l, relL,
                  IfThenElse(Cmp(OpEq, sr("s_suppkey"), lr("l_suppkey")),
                    ForeachUnion(co, BagVarRef(ri), 
                      IfThenElse(Cmp(OpEq, cor("o_orderkey"), lr("l_orderkey")),
                        Singleton(Tuple("c_name2" -> cor("c_name")))))))))) 

  val query = Sequence(List(Named(ri, resultInner), result))
}

/**
  * Let partsuppliers = For ps in PS Union
  * For s in S Union
  *   If (s.s_suppkey = ps.ps_suppkey) // skew join
  *   Then Sng((p_partkey := p.p_partkey, s_name := s.s_name, s_nationkey := s.s_nationkey))
  *
  * Let orders = For o in O Union
  * For c in C Union
  *   If (c.c_custkey = o.o_custkey) // skew join
  *   Then Sng((o_orderkey := o.o_orderkey, c_name := c.c_name, c_nationkey := c.c_nationkey))))
  *
  * Let cparts = For o in orders Union
  *   For l in Lineitem Union
  *     If (o.o_orderkey = l.l_orderkey) 
  *     Then Sng((l_partkey := l.l_partkey, c_name := o.c_name, c_nationkey := o.c_nationkey))
  *
  * // the two joins on the same level should translate to a co-group operation
  * For p in P Union
  *   Sng((p_name := p.p_name, 
  *           suppliers := 
  *             For pss in partsuppliers Union
  *                If (pss.ps_partkey = p.p_partkey)
  *                Then Sng((s_name := s.s_name, s_nationkey := pss.s_nationkey)), 
  *           customers := 
  *             For pc in cparts Union 
  *               If (p.p_partkey = pc.l_partkey)
  *               Then Sng(c_name := pc.c_name, pc.c_nationkey))
  **/
object TPCHQuery3 extends TPCHBase {
  val name = "Query3"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "P", "PS", "S").contains(x._1)).values.toList.mkString("")}"
  
  val partsuppliers = ForeachUnion(ps, relPS,
                    ForeachUnion(s, relS,
                      IfThenElse(Cmp(OpEq, sr("s_suppkey"), psr("ps_suppkey")),
                        Singleton(Tuple("ps_partkey" -> psr("ps_partkey"), 
                                        "s_name" -> sr("s_name"), 
                                        "s_nationkey" -> sr("s_nationkey"))))))
  
  val (psjs, pss, pssr) = varset("partsuppliers", "pss", partsuppliers)

  val custorders = ForeachUnion(o, relO,
                ForeachUnion(c, relC,
                  IfThenElse(Cmp(OpEq, cr("c_custkey"), or("o_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), 
                                    "c_name" -> cr("c_name"), 
                                    "c_nationkey" -> cr("c_nationkey"))))))
  
  val (ojc, co, cor) = varset("custorders", "co", custorders)

  val cparts = ForeachUnion(co, BagVarRef(ojc), 
                ForeachUnion(l, relL,
                  IfThenElse(Cmp(OpEq, cor("o_orderkey"), lr("l_orderkey")),
                    Singleton(Tuple("l_partkey" -> lr("l_partkey"), 
                                    "c_name" -> cor("c_name"),
                                    "c_nationkey" -> cor("c_nationkey"))))))

  val (cpjl, cp, cpr) = varset("cparts", "cp", cparts)
  val partscust = ForeachUnion(p, relP, 
                Singleton(Tuple("p_name" -> pr("p_name"),
                                "suppliers" -> ForeachUnion(pss, BagVarRef(psjs),
                                                IfThenElse(Cmp(OpEq, pssr("ps_partkey"), pr("p_partkey")),
                                                  Singleton(Tuple("s_name" -> pssr("s_name"),
                                                                  "s_nationkey" -> pssr("s_nationkey"))))),
                                "customers" -> ForeachUnion(cp, BagVarRef(cpjl), 
                                                IfThenElse(Cmp(OpEq, cpr("l_partkey"), pr("p_partkey")),
                                                  Singleton(Tuple("c_name" -> cpr("c_name"), 
                                                                  "c_nationkey" -> cpr("c_nationkey"))))))))             
  val query = Sequence(List(Named(psjs, partsuppliers), Named(ojc, custorders), Named(cpjl, cparts), partscust))              
}

object TPCHQuery4Inputs extends TPCHBase {
  
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val name = "CustOrders"
  // input data
  val query = ForeachUnion(c, relC,
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO,
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")),
                    Singleton(Tuple("o_orderkey" -> or("o_orderkey"), "o_orderdate" -> or("o_orderdate")))))))) 
  
}

object TPCHQuery4 extends TPCHBase {
  val name = "Query4"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => List("C", "O", "L", "P").contains(x._1)).values.toList.mkString("")}"

  val custorders = TPCHQuery4Inputs.query.asInstanceOf[BagExpr]
  val (cos1, co1, cor1) = varset(TPCHQuery4Inputs.name, "c", custorders)
  val (cos2, co2, cor2) = varset("orders", "o", BagProject(cor1, "c_orders"))

  val partcnts = GroupBy(ForeachUnion(l, relL,
                     ForeachUnion(p, relP, 
                       IfThenElse(Cmp(OpEq, lr("l_partkey"), pr("p_partkey")), 
                         Singleton(Tuple("l_orderkey" -> lr("l_orderkey"), 
                                         "p_name" -> pr("p_name"), 
                                         "l_qty" -> lr("l_quantity")))))), List("l_orderkey", "p_name"), List("l_qty"), DoubleType)
  
  val (pcs, pc, pcr) = varset("partcnts", "pc", partcnts)

  val custcnts = GroupBy(ForeachUnion(co1, BagVarRef(cos1), 
                ForeachUnion(co2, BagProject(cor1, "c_orders"), 
                  ForeachUnion(pc, BagVarRef(pcs),
                    IfThenElse(Cmp(OpEq, pcr("l_orderkey"), cor2("o_orderkey")),
                      Singleton(Tuple("c_name" -> cor1("c_name"), 
                                      "o_orderdate" -> cor2("o_orderdate"), 
                                      "p_name" -> pcr("p_name"), 
                                      "l_qty" -> pcr("_2"))))))), 
                List("c_name", "o_orderdate", "p_name"),
                List("l_qty"),
                DoubleType)
  val query = Sequence(List(Named(pcs, partcnts), custcnts))

}

object TPCHQuery6Full extends TPCHBase {
  val name = "Query6Full"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "S").contains(x._1)).values.toList.mkString("")}"
 
  val (q2, co, cor) = varset(TPCHQuery2Full.name, "co", TPCHQuery2Full.query.asInstanceOf[BagExpr])

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

/**object TPCHQuery7Full extends TPCHBase {

  val name = "Query7Full"
  def inputs(tmap: Map[String, String]): String = 
    s"val tpch = TPCHLoader(spark)\n${tmap.filter(x => 
      List("C", "O", "L", "P", "PS", "S", "N").contains(x._1)).values.toList.mkString("")}"

  val (q3, co, cor) = varset(TPCHQuery3.name, "co", TPCHQuery3.query.asInstanceOf[BagExpr]) 
   
  val suppliersCond1 = ForeachUnion(sdef, rq3("suppliers").asInstanceOf[BagExpr],
                        IfThenElse(Cmp(OpEq, sref("s_nationkey"), nref("n_nationkey")),
                                   Singleton(Tuple("count" -> Const(1, IntType)))).asInstanceOf[BagExpr])

  val customersCond1 = ForeachUnion(cdef, rq3("customers").asInstanceOf[BagExpr],
                        IfThenElse(Cmp(OpEq, cref("c_nationkey"), nref("n_nationkey")),
                                   Singleton(Tuple("count" -> Const(1, IntType)))).asInstanceOf[BagExpr])
  
  val query = ForeachUnion(n, relN, 
                Singleton(Tuple("n_name" -> nr("n_name"), "parts" -> 
                  ForeachUnion(co, BagVarRef(q3),
                    Total(ForeachUnion           
}**/
