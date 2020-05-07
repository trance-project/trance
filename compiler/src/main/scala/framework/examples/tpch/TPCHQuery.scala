package framework.examples.tpch

import framework.common._
import framework.examples.Query
import framework.utils.Utils.Symbol


/** Base trait for a TPCH query. 
  * Provides support functions for projection tuples, and referencing base tables.
  */
trait TPCHBase extends Query {

  // append other type maps
  def inputTypes(shred: Boolean = false): Map[Type, String] = TPCHSchema.tpchInputs.map(f => translate(f._1) -> f._2)

  def headerTypes(shred: Boolean = false): List[String] =
    inputTypes(shred).values.toList

  def projectBaseTuple(tr: TupleVarRef, omit: List[String] = Nil): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1))))

  def projectTuples(tr1: TupleVarRef, tr2: TupleVarRef, omit: List[String] = Nil): BagExpr = {
    val m1 = tr1.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr1(f._1))
    val m2 = tr2.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr2(f._1))
    Singleton(Tuple(m1 ++ m2))
  }

  def projectTuple(tr: TupleVarRef, nbag:Map[String, TupleAttributeExpr], omit: List[String]): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) ++ nbag))

  def projectTuple(tr: TupleVarRef, nbag:(String, TupleAttributeExpr), omit: List[String] = Nil): BagExpr = 
    Singleton(Tuple(tr.tp.attrTps.withFilter(f => 
      !omit.contains(f._1)).map(f => f._1 -> tr(f._1)) + nbag))

  val relC = BagVarRef("C", TPCHSchema.customertype)
  val cr = TupleVarRef("c", TPCHSchema.customertype.tp)

  val relO = BagVarRef("O", TPCHSchema.orderstype)
  val or = TupleVarRef("o", TPCHSchema.orderstype.tp)

  val relL = BagVarRef("L", TPCHSchema.lineittype)
  val lr = TupleVarRef("l", TPCHSchema.lineittype.tp)

  val relP = BagVarRef("P", TPCHSchema.parttype)
  val pr = TupleVarRef("p", TPCHSchema.parttype.tp)

  val relS = BagVarRef("S", TPCHSchema.suppliertype)
  val sr = TupleVarRef("s", TPCHSchema.suppliertype.tp)
  
  val relPS = BagVarRef("PS", TPCHSchema.partsupptype)
  val psr = TupleVarRef("ps", TPCHSchema.partsupptype.tp)

  val relN = BagVarRef("N", TPCHSchema.nationtype)
  val nr = TupleVarRef("n", TPCHSchema.nationtype.tp)

  val relR = BagVarRef("R", TPCHSchema.regiontype)
  val rr = TupleVarRef("r", TPCHSchema.regiontype.tp)

}
