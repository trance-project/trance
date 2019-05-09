package shredding.algebra

sealed trait Expr {
  def tp: Type
}

// generic data wrapper
case class Input(data: List[Expr]) extends Expr{
  def tp: Type = data match {
    case Nil => BagType(TupleType(Map[String, TupleAttributeType]()))
    case _ => BagType(data.head.tp.asInstanceOf[TupleType]) 
  }
}
case class Const(data: Any) extends Expr{
  def tp: Type = data match {
    case _:Int => IntType
    case _:String => StringType
    case _:Boolean => BoolType
  }
}
case class Sng(e1: Expr) extends Expr {
  def tp: Type = BagType(e1.tp.asInstanceOf[TupleType])
}
case class Tuple(fields: Map[String, Expr]) extends Expr{
  def tp: Type = TupleType(fields.map(f => f._1 -> f._2.tp.asInstanceOf[TupleAttributeType]))
}
case class Equals(e1: Expr, e2: Expr) extends Expr {
  def tp: Type = BoolType
}
case class Lt(e1: Expr, e2: Expr) extends Expr {
  def tp: Type = BoolType
}
case class Gt(e1: Expr, e2: Expr) extends Expr {
  def tp: Type = BoolType
}
case class Project(e1: Expr, field: String) extends Expr {
  def tp: Type = e1.tp.asInstanceOf[TupleType].attrTps(field)
}
case class Select(x: Expr, v: VarDef, p: Expr) extends Expr {
  def tp: Type = x.tp
}
case class Reduce(e1: Expr, v: VarDef, e2: Expr, p: Expr) extends Expr {
  def tp: Type = e2.tp match {
    case t:PrimitiveType => t
    case t:TupleType => BagType(t)
    case _ => sys.error("unsupported type in reduce")
  }
}

case class VarDef(name: String, override val tp: Type) extends Expr { self =>
  override def equals(that: Any): Boolean = that match {
    case that: VarDef => this.name == that.name && this.tp == that.tp
    case _ => false
  }

  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name
}

object VarDef {
  private var lastId = 1
  def fresh(tp: Type): VarDef = {
    val id = lastId
    lastId += 1
    VarDef(s"x$id", tp)
  }
}
