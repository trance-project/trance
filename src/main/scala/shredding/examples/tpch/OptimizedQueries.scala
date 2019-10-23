package shredding.examples.tpch

import shredding.core._
import shredding.nrc.LinearizedNRC
import shredding.wmcc._

trait Query extends NRCTranslator {

  val runner = new PipelineRunner{}
  val normalizer = new Finalizer(new BaseNormalizer{})

  val name: String 
  def inputs(tmap: Map[String, String]): String 
  def inputTypes(shred: Boolean = false): Map[Type, String]
  def headerTypes(shred: Boolean = false): List[String]
  
  /** standard query **/
  val query: Expr
  def calculus: CExpr = translate(query)
  def normalize: CExpr = normalizer.finalize(this.calculus).asInstanceOf[CExpr]
  def unnest: CExpr = Optimizer.applyAll(Unnester.unnest(this.normalize)(Nil, Nil, None))
  def anf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    anfBase.anf(anfer.finalize(this.unnest).asInstanceOf[anfBase.Rep])
  }

  /** shred query **/
  def shred: Expr = runner.shredPipelineNew(query.asInstanceOf[runner.Expr]).asInstanceOf[Expr]
  def scalculus: CExpr = translate(shred)
  def snormalize: CExpr = normalizer.finalize(this.scalculus).asInstanceOf[CExpr]
  def sunnest: CExpr = Optimizer.applyAll(Unnester.unnest(this.snormalize)(Nil, Nil, None))
  def sanf: CExpr = {
    val anfBase = new BaseANF{}
    val anfer = new Finalizer(anfBase)
    println(Printer.quote(this.snormalize))
    println(Printer.quote(this.sunnest))
    anfBase.anf(anfer.finalize(this.sunnest).asInstanceOf[anfBase.Rep])
  }
}

trait TPCHBase extends Query {

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
  val ljp = VarDef("ljp", query1_ljp.tp)                  
  val lp = VarDef("lp", query1_ljp.tp.tp)
  val lpr = TupleVarRef(lp)
  
  val query1 = ForeachUnion(c, relC, 
                Singleton(Tuple("c_name" -> cr("c_name"), "c_orders" -> ForeachUnion(o, relO, 
                  IfThenElse(Cmp(OpEq, or("o_custkey"), cr("c_custkey")), 
                    Singleton(Tuple("o_orderdate" -> or("o_orderdate"), "o_parts" -> 
                      ForeachUnion(lp, BagVarRef(ljp),
                        IfThenElse(Cmp(OpEq, lpr("l_orderkey"), or("o_orderkey")),
                          Singleton(Tuple("p_name" -> lpr("p_name"), "l_qty" -> lpr("l_qty"))))))))))))
  val query = Sequence(List(Named(ljp, query1_ljp), query1))
}
