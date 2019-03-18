package shredding.nrc2

/**
  * Base NRC expressions
  */
trait BaseExpr {

  sealed trait Expr {
    def tp: Type
  }

  trait TupleAttributeExpr extends Expr {
    def tp: TupleAttributeType
  }

  trait LabelAttributeExpr extends TupleAttributeExpr {
    def tp: LabelAttributeType
  }

  trait PrimitiveExpr extends LabelAttributeExpr {
    def tp: PrimitiveType
  }

  trait BagExpr extends TupleAttributeExpr {
    def tp: BagType
  }

  trait TupleExpr extends Expr {
    def tp: TupleType
  }

  trait LabelExpr extends LabelAttributeExpr {
    def tp: LabelAttributeType
  }

}

/**
  * NRC constructs
  */
trait NRC extends BaseExpr with Dictionary {

  case class Const(v: Any, tp: PrimitiveType) extends PrimitiveExpr

  case class BagConst(v: List[Any], tp: BagType) extends BagExpr

  case class VarDef(name: String, tp: Type)

  trait VarRef extends Expr {
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

  case class LabelVarRef(varDef: VarDef) extends LabelExpr with VarRef {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
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

    def tp: TupleAttributeType = tuple.tp.attrs(field)
  }

  case object Project {
    def apply(tuple: TupleExpr, field: String): TupleAttributeExpr = tuple.tp.attrs(field) match {
      case _: PrimitiveType => PrimitiveProject(tuple, field)
      case _: LabelType => LabelProject(tuple, field)
      case _: BagType => BagProject(tuple, field)
      case t => sys.error("Cannot create Project for type " + t)
    }
  }

  case class PrimitiveProject(tuple: TupleExpr, field: String) extends PrimitiveExpr with Project {
    override def tp: PrimitiveType = super.tp.asInstanceOf[PrimitiveType]
  }

  case class LabelProject(tuple: TupleExpr, field: String) extends LabelExpr with Project {
    override def tp: LabelType = super.tp.asInstanceOf[LabelType]
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

  case class Let(x: VarDef, e1: Expr, e2: Expr) extends Expr {
    assert(x.tp == e1.tp)

    val tp: Type = e2.tp
  }

  case class Mult(e1: TupleExpr, e2: BagExpr) extends PrimitiveExpr {
    assert(e1.tp == e2.tp.tp)

    val tp: PrimitiveType = IntType
  }

  case class Cond(op: OpCmp, e1: TupleAttributeExpr, e2: TupleAttributeExpr)

  case class IfThenElse(cond: Cond, e1: BagExpr, e2: Option[BagExpr] = None) extends BagExpr {
    assert(e2.isEmpty || e1.tp == e2.get.tp)

    val tp: BagType = e1.tp
  }

  case class Relation(n: String, tuples: List[Any], tp: BagType) extends BagExpr

  case class NamedBag(n: String, e: BagExpr) extends BagExpr {
    val tp: BagType = e.tp
  }

  /**
    * Shredding NRC extension
    */
  case class Lookup(lbl: LabelExpr, dict: BagDict) extends BagExpr {
    def tp: BagType = dict.flatBagTp

//    def resolve: BagExpr = dict match {
//      case d: OutputBagDict if lbl == d.lbl => d.flat
//      case d: InputBagDict => BagConst(d.f(lbl.asInstanceOf[LabelId]), d.flatBagTp)
//      case d => sys.error("Cannot resolve dictionary lookup " + d)
//    }
  }

  /**
    * Runtime labels appearing in shredded input relations
    */
  object LabelId {
    private var currId = 0

    implicit def orderingById: Ordering[LabelId] = Ordering.by(e => e.id)
  }

  case class LabelId() extends LabelExpr {
    val id: Int = {
      LabelId.currId += 1; LabelId.currId
    }

    def tp: LabelType = LabelType("id" -> IntType)

    override def equals(that: Any): Boolean = that match {
      case that: LabelId => this.id == that.id
      case _ => false
    }

    override def hashCode: Int = id.hashCode()

    override def toString: String = s"LabelId($id)"
  }

  //  case class NewLabel(free: Map[String, VarRef]) extends LabelExpr {
  //    val tp: LabelType = LabelType(free.map(f => f._1 -> f._2.tp.asInstanceOf[LabelAttributeType]))
  //  }


}