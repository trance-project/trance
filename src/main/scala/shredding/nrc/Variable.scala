package shredding.nrc2

/**
  * Variable class for shredding package
  * NRC and Calc have their own wrappers for Variable types
  */

object VarCnt{
  var currId = 0
}

sealed trait VarDef {
  def id: String
  def tp: Type
  def nid: Int
  def n: String
}

private case class PrimitiveVarDef(id: String, tp: PrimitiveType, nid: Int) extends VarDef{
  def n: String = id+nid
}

private case class BagVarDef(id: String, tp: BagType, nid: Int) extends VarDef{
   def n: String = id+nid
}

private case class TupleVarDef(id: String, tp: TupleType, nid: Int) extends VarDef{
  def n: String = id+nid
}

object VarDef {
  def apply(n: String, tp: Type, nid: Int = { VarCnt.currId += 1; VarCnt.currId; }): VarDef = tp match {
      case IntType => PrimitiveVarDef(n, IntType, nid)
      case StringType => PrimitiveVarDef(n, StringType, nid)
      case t: BagType => BagVarDef(n, t, nid)
      case t: TupleType => TupleVarDef(n, t, nid)
      case _ => throw new IllegalArgumentException("cannot create VarDef")
   }
}
