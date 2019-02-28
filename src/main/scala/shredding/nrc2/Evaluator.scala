package shredding.nrc2

/**
  * Simple Scala evaluator
  */

object Evaluator {

  val ctx = collection.mutable.HashMap[String, Any]()
  
  def eval(e: Cond): Boolean = e.op match {
    case OpEq => (eval(e.e1) == eval(e.e2))
    case OpNe => (eval(e.e1) != eval(e.e2))
    case OpGt => (eval(e.e1).asInstanceOf[Int] > eval(e.e2).asInstanceOf[Int])
    case OpGe => (eval(e.e1).asInstanceOf[Int] >= eval(e.e2).asInstanceOf[Int])
  }

  def eval(e: Expr): Any = e match {
    case Const(v, tp) => tp match {
      case IntType => v.toInt
      case StringType => v
    }
    case v: VarRef => v.field match {
      case Some(f) => ctx(v.n).asInstanceOf[Map[String, _]](f)
      case None => ctx(v.n)
    }
    case ForeachUnion(x, e1, e2) =>
      val r1 = eval(e1).asInstanceOf[List[_]]
      val r = r1.flatMap { x1 => ctx(x.n) = x1; eval(e2).asInstanceOf[List[_]] }
      ctx.remove(x.n)
      r
    case Union(e1, e2) =>
      eval(e1).asInstanceOf[List[_]] ++ eval(e2).asInstanceOf[List[_]]
    case Singleton(e1) => List(eval(e1))
    case Tuple(fs) =>
      fs.map(x => x._1 -> eval(x._2))
    case Let(x, e1, e2) =>
      ctx(x.n) = eval(e1)
      val r = eval(e2)
      ctx.remove(x.n)
      r
    case Mult(e1, e2) =>
      val ev1 = eval(e1)
      eval(e2).asInstanceOf[List[_]].count(_ == ev1)
    case IfThenElse(cond, e1, e2) => e2 match {
      case Some(e3) => if (!cond.map{eval(_)}.contains(false)) eval(e1) else eval(e3)
      case None => if (!cond.map{eval(_)}.contains(false)) eval(e1) else Nil
    }
    case PhysicalBag(_, v) => v.map(eval)
    case Relation(_, b) => eval(b)
    case _ => throw new IllegalArgumentException("unknown type")
  }
}
