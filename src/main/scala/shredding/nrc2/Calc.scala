package shredding.nrc2

/**
  * Comprehension calculus
  */

sealed trait Calc { def tp: Type }
trait AttributeCalc extends Calc { def tp: AttributeType }
trait PrimitiveCalc extends AttributeCalc{ def tp: PrimitiveType }
trait BagCalc extends AttributeCalc { def tp: BagType }
trait TupleCalc extends Calc { def tp: TupleType }

/**
  * Comprehension calculus constructs
  */
    
case class Constant(x: String, tp: PrimitiveType) extends PrimitiveCalc

trait Var extends Calc {
  def n: String
  def field: Option[String]
  def tp: Type
}
trait AttributeVar extends AttributeCalc with Var { def tp: AttributeType }
case class PrimitiveVar(n: String, field: Option[String], tp: PrimitiveType) extends PrimitiveCalc with AttributeVar
case class BagVar(n: String, field: Option[String], tp: BagType) extends BagCalc with AttributeVar
case class TupleVar(n: String, field: Option[String], tp: TupleType) extends TupleCalc with Var

object Var {
  def apply(d: VarDef): Var = d match {
    case v: PrimitiveVarDef => PrimitiveVar(v.n, None, v.tp)
    case v: BagVarDef => BagVar(v.n, None, v.tp)
    case v: TupleVarDef => TupleVar(v.n, None, v.tp)
    case _ => throw new IllegalArgumentException(s"cannot create Var(${d.n})")
  }

  def apply(d: Var, f: String): AttributeVar = d match {
    case v: TupleVar => v.tp.tps(f) match {
      case IntType => PrimitiveVar(v.n, Some(f), IntType)
      case StringType => PrimitiveVar(v.n, Some(f), StringType)
      case BoolType => PrimitiveVar(v.n, Some(f), BoolType)
      case t: BagType => BagVar(v.n, Some(f), t)
    }
    case _ => throw new IllegalArgumentException(s"cannot create Var(${d.n}, $f)")
  }

}

// x <- e
case class Generator(x: VarDef, e: BagCalc) extends BagCalc {
  assert(x.tp == e.tp.tp)
  val tp: BagType = e.tp
}
// v <- {e} => v bind e (N6)
// { e1 | ..., v <- {e | r }, .. } => {e1 | ..., r, v bind e, ... } (N8)
case class Bind(x: VarDef, v: TupleCalc) extends TupleCalc{
  val tp: TupleType = v.tp
}
// e1 op e2 
case class Conditional(op: OpCmp, e1: AttributeCalc, e2: AttributeCalc)
case class Pred(cond: Conditional) extends PrimitiveCalc { val tp: PrimitiveType = BoolType }
// necessary for defining outer operators
// ^{ not(p(v,w)) | v != null, w <- Y }
case class AndComp(e: PrimitiveVar, qs: List[AttributeCalc]) extends PrimitiveCalc{
  val tp: PrimitiveType = BoolType
}
// { e | qs ... }
case class BagComp(e: TupleCalc, qs: List[Calc]) extends BagCalc{
  val tp: BagType = BagType(e.tp)
}
// e1 U e2
case class Merge(e1: BagCalc, e2: BagCalc) extends BagCalc {
  // in the future cast based on weaker/stronger types
  assert(e1.tp == e2.tp)
  val tp: BagType = e1.tp
}
// { e }
case class Sng(e: TupleCalc) extends BagCalc { val tp: BagType = BagType(e.tp) }
// { }
case class Zero() extends BagCalc{
  val tp: BagType = BagType(TupleType())
}

// (A1 = e1, ..., An = en)
case class Tup(fields: Map[String, AttributeCalc]) extends TupleCalc {
  val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
}
object Tup{
  def apply(fs: (String, AttributeCalc)*): Tup = Tup(Map(fs:_*))
}

// if pred then { e1 | qs1 ... } else { e2 | qs2 ... }
case class IfStmt(cond: List[Conditional], e1: BagCalc, e2: Option[BagCalc] = None) extends BagCalc {
  assert(e2.isEmpty || e1.tp == e2.get.tp) 
  val tp: BagType = e1.tp
}

case class InputR(n: String, b: PhysicalBag) extends BagCalc{
  val tp: BagType = b.tp
}
case class CLabel(vars: List[Var], flat: BagCalc) extends BagCalc{
  val tp: BagType = flat.tp
}
