package shredding.nrc

import shredding.core._

/**
  * Base NRC expressions
  */
trait BaseExpr {

  trait Expr extends Serializable {
    def tp: Type
  }

  trait TupleAttributeExpr extends Expr {
    def tp: TupleAttributeType
  }

  trait PrimitiveExpr extends TupleAttributeExpr {
    def tp: PrimitiveType
  }

  trait BagExpr extends TupleAttributeExpr {
    def tp: BagType
  }

  trait TupleExpr extends Expr {
    def tp: TupleType
  }

}

/**
  * NRC constructs
  */
trait NRC extends BaseExpr {

  final case class Const(v: Any, tp: PrimitiveType) extends PrimitiveExpr

  trait VarRef {
    def varDef: VarDef

    def name: String = varDef.name

    def tp: Type = varDef.tp
  }

  case object VarRef {
    def apply(varDef: VarDef): Expr = varDef.tp match {
      case _: PrimitiveType => PrimitiveVarRef(varDef)
      case _: BagType => BagVarRef(varDef)
      case _: TupleType => TupleVarRef(varDef)
      case t => sys.error("Cannot create VarRef for type " + t)
    }

    def apply(n: String, tp: Type): Expr = apply(VarDef(n, tp))
  }

  final case class PrimitiveVarRef(varDef: VarDef) extends PrimitiveExpr with VarRef {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  final case class BagVarRef(varDef: VarDef) extends BagExpr with VarRef {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  final case class TupleVarRef(varDef: VarDef) extends TupleExpr with VarRef {
    override def tp: TupleType = super.tp.asInstanceOf[TupleType]
  }

  trait Project {
    def tuple: TupleExpr

    def field: String

    def tp: TupleAttributeType = tuple.tp(field)
  }

  implicit class TupleExprOps(tuple: TupleExpr) {
    def apply(field: String): TupleAttributeExpr = tuple match {
      case Tuple(fs) => fs(field)
      case TupleLet(x, e1, Tuple(fs)) =>
        Let(x, e1, fs(field)).asInstanceOf[TupleAttributeExpr]
      case _ => tuple.tp(field) match {
        case _: PrimitiveType => PrimitiveProject(tuple, field)
        case _: BagType => BagProject(tuple, field)
        case t => sys.error("Cannot create Project for type " + t)
      }
    }
  }

  final case class PrimitiveProject(tuple: TupleExpr, field: String) extends PrimitiveExpr with Project {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  final case class BagProject(tuple: TupleExpr, field: String) extends BagExpr with Project {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  final case class ForeachUnion(x: VarDef, e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(x.tp == e1.tp.tp)

    val tp: BagType = e2.tp
  }

  final case class Union(e1: BagExpr, e2: BagExpr) extends BagExpr {
    assert(e1.tp == e2.tp)

    val tp: BagType = e1.tp
  }

  final case class Singleton(e: TupleExpr) extends BagExpr {
    val tp: BagType = BagType(e.tp)
  }

  final case class WeightedSingleton(e: TupleExpr, w: PrimitiveExpr) extends BagExpr {
    val tp: BagType = BagType(e.tp)
  }

  final case class Tuple(fields: Map[String, TupleAttributeExpr]) extends TupleExpr {
    val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
  }

  object Tuple {
    def apply(fs: (String, TupleAttributeExpr)*): Tuple = Tuple(Map(fs: _*))
  }

  trait Let extends Expr {
    def x: VarDef

    def e1: Expr

    def e2: Expr
  }

  object Let {
    def apply(x: VarDef, e1: Expr, e2: Expr): Expr = e2.tp match {
      case _: PrimitiveType => PrimitiveLet(x, e1, e2.asInstanceOf[PrimitiveExpr])
      case _: TupleType => TupleLet(x, e1, e2.asInstanceOf[TupleExpr])
      case _: BagType => BagLet(x, e1, e2.asInstanceOf[BagExpr])
      case t => sys.error("Cannot create Let for type " + t)
    }
  }

  final case class PrimitiveLet(x: VarDef, e1: Expr, e2: PrimitiveExpr) extends PrimitiveExpr with Let {
    assert(x.tp == e1.tp)

    val tp: PrimitiveType = e2.tp
  }

  final case class TupleLet(x: VarDef, e1: Expr, e2: TupleExpr) extends TupleExpr with Let {
    assert(x.tp == e1.tp)

    val tp: TupleType = e2.tp
  }

  final case class BagLet(x: VarDef, e1: Expr, e2: BagExpr) extends BagExpr with Let {
    assert(x.tp == e1.tp)

    val tp: BagType = e2.tp
  }

  final case class Total(e: BagExpr) extends PrimitiveExpr {
    val tp: PrimitiveType = IntType
  }

  final case class DeDup(e: BagExpr) extends BagExpr {
    val tp: BagType = e.tp
  }

  trait Cond extends PrimitiveExpr {
    def tp: PrimitiveType = BoolType
  }

  final case class Cmp(op: OpCmp, e1: TupleAttributeExpr, e2: TupleAttributeExpr) extends Cond

  final case class And(e1: Cond, e2: Cond) extends Cond

  final case class Or(e1: Cond, e2: Cond) extends Cond

  final case class Not(c: Cond) extends Cond

  trait IfThenElse {
    def cond: Cond

    def e1: Expr

    def e2: Option[Expr]
  }

  object IfThenElse {
    def apply(c: Cond, e1: Expr, e2: Expr): Expr = e1.tp match {
      case _: PrimitiveType =>
        PrimitiveIfThenElse(c, e1.asInstanceOf[PrimitiveExpr], e2.asInstanceOf[PrimitiveExpr])
      case _: TupleType =>
        TupleIfThenElse(c, e1.asInstanceOf[TupleExpr], e2.asInstanceOf[TupleExpr])
      case _: BagType =>
        BagIfThenElse(c, e1.asInstanceOf[BagExpr], Some(e2.asInstanceOf[BagExpr]))
      case t => sys.error("Cannot create IfThenElse for type " + t)
    }

    def apply(c: Cond, e1: Expr): BagIfThenElse = e1.tp match {
      case _: BagType => BagIfThenElse(c, e1.asInstanceOf[BagExpr], None)
      case t => sys.error("Cannot create IfThen for type " + t)
    }
  }

  final case class PrimitiveIfThenElse(cond: Cond, e1: PrimitiveExpr, p2: PrimitiveExpr) extends PrimitiveExpr with IfThenElse {
    assert(e1.tp == p2.tp)

    val tp: PrimitiveType = e1.tp

    def e2: Option[PrimitiveExpr] = Some(p2)
  }

  final case class TupleIfThenElse(cond: Cond, e1: TupleExpr, t2: TupleExpr) extends TupleExpr with IfThenElse {
    assert(e1.tp == t2.tp)

    val tp: TupleType = e1.tp

    def e2: Option[TupleExpr] = Some(t2)
  }

  final case class BagIfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr]) extends BagExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

}
