package shredding.nrc

/**
  * NRC source language
  */

sealed trait Expr { def tp: Type }
trait AttributeExpr extends Expr { def tp: AttributeType }
trait PrimitiveExpr extends AttributeExpr { def tp: PrimitiveType }
trait BagExpr extends AttributeExpr { def tp: BagType }
trait TupleExpr extends Expr { def tp: TupleType }

/**
  * NRC constructs
  */
case class Const(v: String, tp: PrimitiveType) extends PrimitiveExpr

trait VarRef extends Expr {
  def n: String
  def field: Option[String]
  def tp: Type
}
trait AttributeVarRef extends AttributeExpr with VarRef { def tp: AttributeType }
case class PrimitiveVarRef(n: String, field: Option[String], tp: PrimitiveType) extends PrimitiveExpr with AttributeVarRef
case class BagVarRef(n: String, field: Option[String], tp: BagType) extends BagExpr with AttributeVarRef
case class TupleVarRef(n: String, field: Option[String], tp: TupleType) extends TupleExpr with VarRef

object VarRef {
  def apply(d: VarDef): VarRef = d match {
    case v: PrimitiveVarDef => PrimitiveVarRef(v.n, None, v.tp)
    case v: BagVarDef => BagVarRef(v.n, None, v.tp)
    case v: TupleVarDef => TupleVarRef(v.n, None, v.tp)
    case _ => throw new IllegalArgumentException(s"cannot create VarRef(${d.n})")
  }

  def apply(d: VarDef, f: String): AttributeVarRef = d match {
    case v: TupleVarDef => v.tp.tps(f) match {
      case IntType => PrimitiveVarRef(v.n, Some(f), IntType)
      case StringType => PrimitiveVarRef(v.n, Some(f), StringType)
      case BoolType => PrimitiveVarRef(v.n, Some(f), BoolType)
      case t: BagType => BagVarRef(v.n, Some(f), t)
    }
    case _ => throw new IllegalArgumentException(s"cannot create VarRef(${d.n}, $f)")
  }
}

case class ForeachUnion(x: VarDef, e1: BagExpr, e2: BagExpr) extends BagExpr {
  assert(x.tp == e1.tp.tp)

  val tp: BagType = e2.tp
}

case class Union(e1: BagExpr, e2: BagExpr) extends BagExpr {
  assert(e1.tp == e2.tp)

  val tp: BagType = e1.tp
}

case class Singleton(e: TupleExpr) extends BagExpr {
  val tp: BagType = BagType(e.tp)
}

case class Tuple(fields: Map[String, AttributeExpr]) extends TupleExpr {
  val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
}

object Tuple {
  def apply(fs: (String, AttributeExpr)*): Tuple = Tuple(Map(fs: _*))
}

case class Let(x: VarDef, e1: Expr, e2: Expr) extends Expr {
  assert(x.tp == e1.tp)

  val tp: Type = e2.tp
}

case class Mult(e1: TupleExpr, e2: BagExpr) extends PrimitiveExpr {
  assert(e1.tp == e2.tp.tp)

  val tp: PrimitiveType = IntType
}

case class Cond(op: OpCmp, e1: AttributeExpr, e2: AttributeExpr)

case class IfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr] = None) extends BagExpr {
  assert(e2.isEmpty || e1.tp == e2.get.tp)

  val tp: BagType = e1.tp
}

case class PhysicalBag(tp: BagType, values: List[TupleExpr]) extends BagExpr

object PhysicalBag {
  def apply(itemTp: TupleType, values: TupleExpr*): PhysicalBag =
    PhysicalBag(BagType(itemTp), List(values: _*))
}

case class Relation(n: String, b: PhysicalBag) extends BagExpr {
  val tp: BagType = b.tp
}

case class Label(vars: List[VarRef], flat: BagExpr) extends BagExpr {
  val tp: BagType = flat.tp
}
