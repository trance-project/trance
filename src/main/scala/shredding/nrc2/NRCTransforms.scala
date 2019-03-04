package shredding.nrc2

trait NRCTransforms {
  this: NRC =>

  /**
    * Pretty printer
    */
  object Printer {

    import shredding.Utils.ind

    def quote(e: Expr): String = e match {
      case Const(v, StringType) => "\"" + v + "\""
      case Const(v, _) => v
      case VarRef(v) => v.name
      case Project(v, f) => v.name + "." + f
      case ForeachUnion(x, e1, e2) =>
        s"""|For ${x.name} in ${quote(e1)} Union
            |${ind(quote(e2))}""".stripMargin
      case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
      case Singleton(e1) => "sng(" + quote(e1) + ")"
      case Tuple(fs) => s"( ${fs.map(f => f._1 + " := " + quote(f._2)).mkString(", ")} )"
      case Let(x, e1, e2) =>
        s"""|Let ${x.name} = ${quote(e1)} In
            |${ind(quote(e2))}""".stripMargin
      case Mult(e1, e2) => s"Mult(${quote(e1)}, ${quote(e2)})"
      case IfThenElse(Cond(op, l, r), e1, None) =>
        s"""|If (${quote(l)} $op ${quote(r)})
            |Then ${quote(e1)}""".stripMargin
      case IfThenElse(Cond(op, l, r), e1, Some(e2)) =>
        s"""|If (${quote(l)} $op ${quote(r)})
            |Then ${quote(e1)}
            |Else ${quote(e2)}""".stripMargin
      case Relation(n, _, _) => n
      case NewLabel(vs) => s"Label(vars := ${vs.map(Printer.quote).mkString(", ")})"
      case Lookup(lbl, dict) => s"pi_1(${quote(dict)})($lbl)"
      case _ => throw new IllegalArgumentException("unknown type")
    }

    def quote(d: Dict): String = d match {
      case EmptyDict => "Empty dictionary"
      case BagDict(lbl, flat, dict) =>
        s"""|( ${quote(lbl)} --> ${quote(flat)},
            |${ind(quote(dict))}
            |""".stripMargin
      case TupleDict(fs) => s"(${fs.map(f => f._1 + " := " + quote(f._2)).mkString(", ")})"
      case _ => throw new IllegalArgumentException("Illegal dictionary")
    }
  }

  /**
    * Simple Scala evaluator
    */
  class Evaluator {

    import collection.mutable.{HashMap => HMap}

    val ctx: HMap[String, Any] = HMap[String, Any]()

    def eval(e: Expr): Any = e match {
      case Const(v, IntType) => v.toInt
      case Const(v, StringType) => v
      case VarRef(v) => ctx(v.name)
      case Project(v, f) => ctx(v.name).asInstanceOf[Map[String, _]](f)
      case ForeachUnion(x, e1, e2) =>
        val v1 = eval(e1).asInstanceOf[List[_]]
        val v = v1.flatMap { xv => ctx(x.name) = xv; eval(e2).asInstanceOf[List[_]] }
        ctx.remove(x.name)
        v
      case Union(e1, e2) => eval(e1).asInstanceOf[List[_]] ++ eval(e2).asInstanceOf[List[_]]
      case Singleton(e1) => List(eval(e1))
      case Tuple(fs) => fs.map(x => x._1 -> eval(x._2))
      case Let(x, e1, e2) =>
        ctx(x.name) = eval(e1)
        val v = eval(e2)
        ctx.remove(x.name)
        v
      case Mult(e1, e2) =>
        val v1 = eval(e1)
        eval(e2).asInstanceOf[List[_]].count(_ == v1)
      case IfThenElse(Cond(op, l, r), e1, None) =>
        val vl = eval(l)
        val vr = eval(r)
        op match {
          case OpEq => if (vl == vr) eval(e1) else Nil
          case OpNe => if (vl != vr) eval(e1) else Nil
        }
      case IfThenElse(Cond(op, l, r), e1, Some(e2)) =>
        val vl = eval(l)
        val vr = eval(r)
        op match {
          case OpEq => if (vl == vr) eval(e1) else eval(e2)
          case OpNe => if (vl != vr) eval(e1) else eval(e2)
        }
      case Relation(_, _, b) => b
      case _ => throw new IllegalArgumentException("unknown type")
    }
  }

}
