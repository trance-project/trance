package shredding.algebra

sealed trait Expr {
  def tp: Type
}
case class Input(data: Any) extends Expr {
  def tp: Type = BagType(TupleType(Map("a" -> IntType))) // TODO
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
case class Project(e1: Expr, pos: Int) extends Expr {
  def tp: Type = e1.tp // TODO
}
case class Select(x: Expr, v: VarDef, p: Expr) extends Expr {
  def tp: Type = x.tp
}
case class Reduce(e: Expr, p: Expr => Expr) extends Expr {
  def tp: Type = e.tp // TODO
}
case class Plan(e1: Expr, e2: Expr) extends Expr {
  def tp: Type = e1.tp // TODO
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
