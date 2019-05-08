package shredding.algebra

sealed trait Expr

case class Input(data: Any) extends Expr
case class Equals(e1: Expr, e2: Expr) extends Expr
case class Lt(e1: Expr, e2: Expr) extends Expr
case class Gt(e1: Expr, e2: Expr) extends Expr
case class Project(e1: Expr, pos: Int) extends Expr
case class Select(x: Expr, p: Expr => Expr) extends Expr
case class Reduce(e: Expr, p: Expr => Expr) extends Expr
case class Plan(e1: Expr, e2: Expr) extends Expr
