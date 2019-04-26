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
      case t => sys.error("Cannot create VarRef for type " + t)
    }

    def apply(n: String, tp: Type): VarRef = apply(VarDef(n, tp))
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

  trait Project extends Expr {
    def tuple: TupleExpr

    def field: String

    def tp: TupleAttributeType = tuple.tp.attrTps(field)
  }

  implicit class TupleExprOps(tuple: TupleExpr) {
    def apply(field: String): TupleAttributeExpr = tuple match {
      case t: Tuple => t.fields(field)
      case _ => tuple.tp.attrTps(field) match {
        case _: PrimitiveType => PrimitiveProject(tuple, field)
        case _: BagType => BagProject(tuple, field)
        case t => sys.error("Cannot create Project for type " + t)
      }
    }
  }

  case class PrimitiveProject(tuple: TupleExpr, field: String) extends PrimitiveExpr with Project {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class BagProject(tuple: TupleExpr, field: String) extends BagExpr with Project {
    override def tp: BagType = super.tp.asInstanceOf[BagType]
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

  object Let {
    def apply(x: VarDef, e1: Expr, e2: Expr): Let = e2.tp match {
      case _: PrimitiveType => PrimitiveLet(x, e1, e2.asInstanceOf[PrimitiveExpr])
      case _: TupleType => TupleLet(x, e1, e2.asInstanceOf[TupleExpr])
      case _: BagType => BagLet(x, e1, e2.asInstanceOf[BagExpr])
      case t => sys.error("Cannot create Let for type " + t)
    }
  }

  trait Let extends Expr {
    def x: VarDef

    def e1: Expr

    def e2: Expr
  }

  case class PrimitiveLet(x: VarDef, e1: Expr, e2: PrimitiveExpr) extends PrimitiveExpr with Let {
    assert(x.tp == e1.tp)

    val tp: PrimitiveType = e2.tp
  }

  case class TupleLet(x: VarDef, e1: Expr, e2: TupleExpr) extends TupleExpr with Let {
    assert(x.tp == e1.tp)

    val tp: TupleType = e2.tp
  }

  case class BagLet(x: VarDef, e1: Expr, e2: BagExpr) extends BagExpr with Let {
    assert(x.tp == e1.tp)

    val tp: BagType = e2.tp
  }

  case class Total(e: BagExpr) extends PrimitiveExpr {
    val tp: PrimitiveType = IntType
  }

  case class Cond(op: OpCmp, e1: TupleAttributeExpr, e2: TupleAttributeExpr)

  trait IfThenElse extends Expr {
    def cond: Cond

    def e1: Expr

    def e2: Option[Expr]
  }

  object IfThenElse {
    def apply(c: Cond, e1: Expr, e2: Expr): IfThenElse = e1.tp match {
      case _: PrimitiveType =>
        PrimitiveIfThenElse(c, e1.asInstanceOf[PrimitiveExpr], Some(e2.asInstanceOf[PrimitiveExpr]))
      case _: TupleType =>
        TupleIfThenElse(c, e1.asInstanceOf[TupleExpr], Some(e2.asInstanceOf[TupleExpr]))
      case _: BagType =>
        BagIfThenElse(c, e1.asInstanceOf[BagExpr], Some(e2.asInstanceOf[BagExpr]))
      case t => sys.error("Cannot create IfThenElse for type " + t)
    }

    def apply(c: Cond, e1: BagExpr): BagIfThenElse = BagIfThenElse(c, e1, None)
  }

  case class PrimitiveIfThenElse(cond: Cond, e1: PrimitiveExpr, e2: Option[PrimitiveExpr]) extends PrimitiveExpr with IfThenElse {
    assert(e2.isDefined && e1.tp == e2.get.tp)

    val tp: PrimitiveType = e1.tp
  }

  case class TupleIfThenElse(cond: Cond, e1: TupleExpr, e2: Option[TupleExpr]) extends TupleExpr with IfThenElse {
    assert(e2.isDefined && e1.tp == e2.get.tp)

    val tp: TupleType = e1.tp
  }

  case class BagIfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr]) extends BagExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

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
trait ShreddedNRC extends NRC {

  /**
    * Label extensions
    */
  sealed trait LabelExpr extends TupleAttributeExpr with LabelAttributeExpr {
    def tp: LabelType
  }

  case class LabelVarRef(varDef: VarDef) extends LabelExpr with VarRef {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
  }
  case class LabelProject(tuple: TupleExpr, field: String) extends LabelExpr with Project {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
  }

  case class LabelLet(x: VarDef, e1: Expr, e2: LabelExpr) extends LabelExpr with Let {
    assert(x.tp == e1.tp)

    val tp: LabelType = e2.tp
  }

  case class LabelIfThenElse(cond: Cond, e1: LabelExpr, e2: Option[LabelExpr]) extends LabelExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: LabelType = e1.tp
  }

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

    val tp: LabelType =
      LabelType(vars.map(r => r.name -> r.tp.asInstanceOf[LabelAttributeType]).toMap)

    override def equals(that: Any): Boolean = that match {
      case that: Label => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String =
      s"Label(${(id :: vars.map(_.toString).toList).mkString(", ")}"
  }

  /**
    * Dictionary extensions
    */
  sealed trait DictExpr extends LabelAttributeExpr {
    def tp: DictType
  }

  sealed trait TupleDictAttributeExpr extends DictExpr {
    def tp: TupleDictAttributeType
  }

  sealed trait BagDictExpr extends TupleDictAttributeExpr {
    def tp: BagDictType
  }

  sealed trait TupleDictExpr extends DictExpr {
    def tp: TupleDictType
  }

  case class BagDictVarRef(varDef: VarDef) extends BagDictExpr with VarRef {
    override def tp: BagDictType = super.tp.asInstanceOf[BagDictType]
  }

  case class TupleDictVarRef(varDef: VarDef) extends TupleDictExpr with VarRef {
    override def tp: TupleDictType = super.tp.asInstanceOf[TupleDictType]
  }

  case object EmptyDict extends TupleDictAttributeExpr {
    def tp: TupleDictAttributeType = EmptyDictType
  }

  case class BagDict(flat: BagExpr, dict: TupleDictExpr) extends BagDictExpr {
    val tp: BagDictType = BagDictType(flat.tp, dict.tp)
  }

  case class TupleDict(fields: Map[String, TupleDictAttributeExpr]) extends TupleDictExpr {
    val tp: TupleDictType = TupleDictType(fields.map(f => f._1 -> f._2.tp))
  }

  implicit class TupleDictExprOps(d: TupleDictExpr) {
    def apply(field: String): TupleDictAttributeExpr = d match {
      case TupleDict(fs) => fs(field)
      case _ => d.tp.attrTps(field) match {
        case EmptyDictType => EmptyDict
        case _: BagDictType => BagDictProject(d, field)
      }
    }
  }

  case class BagDictProject(dict: TupleDictExpr, field: String) extends BagDictExpr {
    val tp: BagDictType = dict.tp.attrTps(field).asInstanceOf[BagDictType]
  }

  implicit class BagDictExprOps(d: BagDictExpr) {
    def lookup(lbl: LabelExpr): BagExpr = d match {
      case BagDict(f, _) => f
      case _ => Lookup(lbl, d)
    }

    def tupleDict: TupleDictExpr = d match {
      case BagDict(_, td) => td
      case _ => DictProjectInBagDict(d)
    }
  }

  case class DictLet(x: VarDef, e1: Expr, e2: DictExpr) extends DictExpr with Let {
    assert(x.tp == e1.tp)

    val tp: DictType = e2.tp
  }

  case class DictIfThenElse(cond: Cond, e1: DictExpr, e2: Option[DictExpr]) extends DictExpr with IfThenElse {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: DictType = e1.tp
  }


  case class Lookup(lbl: LabelExpr, dict: BagDictExpr) extends BagExpr {
    def tp: BagType = dict.tp.flatTp
  }

  case class DictProjectInBagDict(dict: BagDictExpr) extends TupleDictExpr {
    val tp: TupleDictType = dict.tp.dictTp
  }

  implicit class DictExprOps(dict1: DictExpr) {
    def union(dict2: DictExpr): DictExpr = (dict1, dict2) match {
      case (EmptyDict, EmptyDict) => EmptyDict
      case (BagDict(f1, d1), BagDict(f2, d2)) =>
        BagDict(Union(f1, f2), d1.union(d2).asInstanceOf[TupleDictExpr])
      case (d1: BagDictExpr, d2: BagDictExpr) =>
        DictUnion(d1, d2)
      case (TupleDict(fields1), TupleDict(fields2)) =>
        assert(fields1.keySet == fields2.keySet)
        TupleDict(fields1.map {
          case (k1, d1) =>
            k1 -> d1.union(fields2(k1)).asInstanceOf[TupleDictAttributeExpr]
        })
      case (d1: TupleDictExpr, d2: TupleDictExpr) =>
        DictUnion(d1, d2)
      case _ => sys.error("Illegal dictionary union " + dict1 + " and " + dict2)
    }
  }

  case class DictUnion(dict1: DictExpr, dict2: DictExpr) extends DictExpr {
    assert(dict1.tp == dict2.tp)

    val tp: DictType = dict1.tp
  }

  case class ShredExpr(flat: Expr, dict: DictExpr)

  case object ShredVarRef {
    def apply(varDef: VarDef): VarRef = varDef.tp match {
      case _: LabelType => LabelVarRef(varDef)
      case _ => VarRef(varDef)
    }
  }

  case object ShredProject {
    def apply(tuple: TupleExpr, field: String): TupleAttributeExpr = tuple.tp.attrTps(field) match {
      case _: LabelType => LabelProject(tuple, field)
      case _ => tuple(field)
    }
  }

  case object ShredLet {
    def apply(x: VarDef, e1: Expr, e2: Expr): Let = e2.tp match {
      case _: LabelType => LabelLet(x, e1, e2.asInstanceOf[LabelExpr])
      case _: DictType => DictLet(x, e1, e2.asInstanceOf[DictExpr])
      case _ => Let(x, e1, e2)
    }
  }

  case object ShredIfThenElse {
    def apply(c: Cond, e1: Expr, e2: Expr): IfThenElse = e1.tp match {
      case _: LabelType =>
        LabelIfThenElse(c, e1.asInstanceOf[LabelExpr], Some(e2.asInstanceOf[LabelExpr]))
      case _: DictType =>
        DictIfThenElse(c, e1.asInstanceOf[DictExpr], Some(e2.asInstanceOf[DictExpr]))
      case _ => IfThenElse(c, e1, e2)
    }

    def apply(c: Cond, e1: BagExpr): BagIfThenElse = BagIfThenElse(c, e1, None)

    def apply(c: Cond, e1: LabelExpr): LabelIfThenElse = LabelIfThenElse(c, e1, None)

    def apply(c: Cond, e1: DictExpr): DictIfThenElse = DictIfThenElse(c, e1, None)
  }

}