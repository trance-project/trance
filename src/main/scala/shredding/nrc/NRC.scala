package shredding.nrc

import shredding.core._

/**
  * Base NRC expressions
  */
trait BaseExpr {

  sealed trait Expr {
    def tp: Type
  }

  sealed trait TupleAttributeExpr extends Expr {
    def tp: TupleAttributeType
  }

  sealed trait LabelAttributeExpr extends Expr {
    def tp: LabelAttributeType
  }

  sealed trait PrimitiveExpr extends TupleAttributeExpr with LabelAttributeExpr {
    def tp: PrimitiveType
  }

  sealed trait BagExpr extends TupleAttributeExpr {
    def tp: BagType
  }

  sealed trait TupleExpr extends Expr with LabelAttributeExpr {
    def tp: TupleType
  }

  sealed trait LabelExpr extends TupleAttributeExpr with LabelAttributeExpr {
    def tp: LabelType
  }

}

/**
  * NRC constructs
  */
trait NRC extends BaseExpr {

  case class Const(v: Any, tp: PrimitiveType) extends PrimitiveExpr

  sealed trait VarRef extends Expr {
    def varDef: VarDef

    def name: String = varDef.name

    def tp: Type = varDef.tp
  }

  case object VarRef {
    def apply(varDef: VarDef): VarRef = varDef.tp match {
      case _: PrimitiveType => PrimitiveVarRef(varDef)
      case _: BagType => BagVarRef(varDef)
      case _: TupleType => TupleVarRef(varDef)
      case _: LabelType => LabelVarRef(varDef)
      case t => sys.error("Cannot create VarRef for type " + t)
    }
  }

  case class PrimitiveVarRef(varDef: VarDef) extends PrimitiveExpr with VarRef {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class BagVarRef(varDef: VarDef) extends BagExpr with VarRef {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  case class TupleVarRef(varDef: VarDef) extends TupleExpr with VarRef {
    override def tp: TupleType = super.tp.asInstanceOf[TupleType]
  }

  case class LabelVarRef(varDef: VarDef) extends LabelExpr with VarRef {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  trait Project extends Expr {
    def tuple: TupleExpr

    def field: String

    def tp: TupleAttributeType = tuple.tp.attrs(field)
  }

  case object Project {
    def apply(tuple: TupleExpr, field: String): TupleAttributeExpr = tuple.tp.attrs(field) match {
      case _: PrimitiveType => PrimitiveProject(tuple, field)
      case _: BagType => BagProject(tuple, field)
      case _: LabelType => LabelProject(tuple, field)
      case t => sys.error("Cannot create Project for type " + t)
    }
  }

  case class PrimitiveProject(tuple: TupleExpr, field: String) extends PrimitiveExpr with Project {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class BagProject(tuple: TupleExpr, field: String) extends BagExpr with Project {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
  }

  case class LabelProject(tuple: TupleExpr, field: String) extends LabelExpr with Project {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
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

  case class Tuple(fields: Map[String, TupleAttributeExpr]) extends TupleExpr {
    val tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp))
  }

  object Tuple {
    def apply(fs: (String, TupleAttributeExpr)*): Tuple = Tuple(Map(fs: _*))
  }

  case class Let(x: VarDef, e1: Expr, e2: Expr) extends Expr {
    assert(x.tp == e1.tp)

    val tp: Type = e2.tp
  }

  case class Total(e: BagExpr) extends PrimitiveExpr {
    val tp: PrimitiveType = IntType
  }

  case class Cond(op: OpCmp, e1: TupleAttributeExpr, e2: TupleAttributeExpr)

  case class IfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr] = None) extends BagExpr {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

  case class InputBag(n: String, tuples: List[Any], tp: BagType) extends BagExpr


  case class Named(n: String, e: Expr) extends Expr {
    val tp: Type = TupleType()    // unit type
  }

  case class Sequence(exprs: List[Expr]) extends Expr {
    val tp: Type = TupleType()    // unit type
  }

}

/**
  * Shredding NRC extension
  */
trait ShreddedNRC extends NRC with Dictionary {

  case class Lookup(lbl: LabelExpr, dict: BagDict) extends BagExpr {
    def tp: BagType = dict.flatBagTp
  }

  /**
    * Runtime labels appearing in shredded input relations
    */
  object Label {
    private var currId = 0

    def getNextId: Int = {
      currId += 1
      currId
    }

    implicit def orderingById: Ordering[Label] = Ordering.by(e => e.id)
  }

  case class Label(vars: Set[VarRef] = Set.empty) extends LabelExpr {
    val id: Int = Label.getNextId

    val tp: LabelType = LabelType(vars.map(r => r.name -> r.tp.asInstanceOf[LabelAttributeType]).toMap)

    override def equals(that: Any): Boolean = that match {
      case that: Label => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String =
      s"Label(${(id :: vars.map(_.toString).toList).mkString(", ")}"
  }

}
