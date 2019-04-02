package shredding.nrc

trait Printer {
  this: NRC =>

  import shredding.Utils.ind

  def quote(e: Expr): String = e match {
    case Const(v, StringType) => "\"" + v + "\""
    case Const(v, _) => v.toString
    case v: VarRef => v.name
    case p: Project => quote(p.tuple) + "." + p.field
    case ForeachUnion(x, e1, e2) =>
      s"""|For ${x.name} in ${quote(e1)} Union
          |${ind(quote(e2))}""".stripMargin
    case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
    case Singleton(e1) => s"Sng(${quote(e1)})"
    case Tuple(fs) =>
      s"(${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(", ")})"
    case Let(x, e1, e2) =>
      s"""|Let ${x.name} = ${quote(e1)} In
          |${ind(quote(e2))}""".stripMargin
    case Total(e1) => s"Total(${quote(e1)})"
    case IfThenElse(Cond(op, l, r), e1, None) =>
      s"""|If (${quote(l)} $op ${quote(r)})
          |Then ${quote(e1)}""".stripMargin
    case IfThenElse(Cond(op, l, r), e1, Some(e2)) =>
      s"""|If (${quote(l)} $op ${quote(r)})
          |Then ${quote(e1)}
          |Else ${quote(e2)}""".stripMargin
    case InputBag(n, _, _) => n
    case Named(n, e1) => s"$n := ${quote(e1)}"
    case Sequence(ee) => ee.map(quote).mkString("\n")
    case _ => sys.error("Cannot print unknown expression " + e)
  }

  def quote(v: Any, tp: Type): String = tp match {
    case IntType => v.toString
    case StringType => "\"" + v.toString + "\""
    case BagType(tp2) =>
      val l = v.asInstanceOf[List[Any]]
      s"""|[
          |${ind(l.map(quote(_, tp2)).mkString(",\n"))}
          |]""".stripMargin
    case TupleType(as) =>
      val m = v.asInstanceOf[Map[String, Any]]
      s"(${m.map { case (n, a) => n + " := " + quote(a, as(n)) }.mkString(", ")})"
    case LabelType(as) =>
      val m = v.asInstanceOf[Map[String, Any]]
      s"Label(${m.map { case (n, a) => n + " := " + quote(a, as(n)) }.mkString(", ")})"
  }
}

trait ShreddedPrinter extends Printer {
  this: ShreddedNRC =>

  import shredding.Utils.ind

  override def quote(e: Expr): String = e match {
    case l: Label =>
      s"Label(${(l.id :: l.vars.toList.map(quote)).mkString(", ")})"
    case Lookup(lbl, dict) =>
      s"Lookup(${quote(dict)})(${quote(lbl)})"
    case _ => super.quote(e)
  }

  def quote(d: Dict): String = d match {
    case EmptyDict => "Nil"
    case InputBagDict(f, _, dict) =>
      s"""|(
          |${ind(f.toString)},
          |${ind(quote(dict))}
          |)""".stripMargin
    case OutputBagDict(lbl, flat, dict) =>
      s"""|(
          |${ind(quote(lbl) + " --> " + quote(flat))},
          |${ind(quote(dict))}
          |)""".stripMargin
    case TupleDict(fs) =>
      s"(${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(", ")})"
  }
}
