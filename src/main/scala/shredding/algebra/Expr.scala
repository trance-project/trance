package shredding.algebra

import shredding.core._

sealed trait CExpr {
  def tp: Type
}

case class InputRef(data: String, tp: Type) extends CExpr
case class Input(data: List[CExpr]) extends CExpr{
  def tp: BagType = data match {
    case Nil => BagType(TupleType(Map[String, TupleAttributeType]()))
    case _ => BagType(data.head.tp.asInstanceOf[TupleType]) 
  }
}

case class Constant(data: Any) extends CExpr{
  def tp: PrimitiveType = data match {
    case _:Int => IntType
    case _:String => StringType
    case _:Boolean => BoolType
  }
}

case class Sng(e1: CExpr) extends CExpr {
  def tp: BagType = BagType(e1.tp.asInstanceOf[TupleType])
}

case class Tuple(fields: Map[String, CExpr]) extends CExpr{
  def tp: TupleType = TupleType(fields.map(f => f._1 -> f._2.tp.asInstanceOf[TupleAttributeType]))
}

case class KVTuple(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: KVTupleType = KVTupleType(e1.tp, e2.tp)
}

case class Equals(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class NEquals(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Lt(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Lte(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Gt(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Gte(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class And(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: PrimitiveType = BoolType
}

case class Not(e1: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Or(e1: CExpr, e2: CExpr) extends CExpr{
  def tp: PrimitiveType = BoolType
}

case class Project(e1: CExpr, field: String) extends CExpr { self =>
  def tp: Type = e1.tp match {
    case t:TupleType => t.attrTps(field)
    case t:KVTupleType => field match {
      case "key" => t.e1
      case "value" => t.e2
    }
    case _ => sys.error("unsupported projection index "+self)
  }
}

case class If(cond: CExpr, e1: CExpr, e2: Option[CExpr]) extends CExpr {
  assert(cond.tp == BoolType)
  // disjoint types?
  val tp: Type = e1.tp
}

case class Merge(e1: CExpr, e2: CExpr) extends CExpr {
  def tp: Type = e1.tp
}

// reorder variables, confusing to create
case class Comprehension(e1: CExpr, v: Variable, p: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp match {
    case t:TupleType => BagType(t)
    case t => t
  }
}

// bind x to e in e1
case class Bind(x: Variable, e1: CExpr, e: CExpr) extends CExpr {
  def tp: Type = e.tp
}



/**
  * Shred extensions
  */


/**
  * Algebra extensions
  */

case class Select(x: CExpr, v: Variable, p: CExpr) extends CExpr {
  def tp: Type = x.tp
}
case class Reduce(e1: CExpr, v: Variable, e2: CExpr, p: CExpr) extends CExpr {
  def tp: Type = e2.tp match {
    case t:TupleType => BagType(t)
    case t => t
  }
}

// { (v1, v2) | v1 <- e1, v2 <- e2(v1), p((v1, v2)) } 
case class Unnest(e1: CExpr, v1: Variable, e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagType(KVTupleType(v1.tp, e2.tp.asInstanceOf[BagType].tp))
}

case class OuterUnnest(e1: CExpr, v1: Variable, e2: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagType(KVTupleType(v1.tp, e2.tp.asInstanceOf[BagType].tp))
}

case class Nest(e1: CExpr, v1: Variable, f: CExpr, e: CExpr, v2: Variable, p: CExpr) extends CExpr {
  def tp: Type = BagType(KVTupleType(f.tp, e.tp))
}

case class OuterJoin(e1: CExpr, e2: CExpr, v1: Variable, p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagType = BagType(KVTupleType(e1.tp, e2.tp))
}

case class Join(e1: CExpr, e2: CExpr, v1: Variable, p1: CExpr, v2: Variable, p2: CExpr) extends CExpr {
  def tp: BagType = BagType(KVTupleType(e1.tp, e2.tp))
}


case class Variable(name: String, override val tp: Type) extends CExpr { self =>
  override def equals(that: Any): Boolean = that match {
    case that: Variable => this.name == that.name && this.tp == that.tp
    case _ => false
  }

  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name

}

object Variable {
  private var lastId = 1
  def fresh(tp: Type): Variable = {
    val id = lastId
    lastId += 1
    Variable(s"x$id", tp)
  }
}
