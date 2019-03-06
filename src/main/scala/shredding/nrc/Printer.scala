package shredding.nrc2

import shredding.Utils.ind

/**
  * Pretty printer
  */
object Printer {

  def quote(e: Expr): String = e match {
    case Const(v, StringType) => "\""+ v +"\""
    case Const(v, _) => v
    case v: VarRef => v.n + v.field.map("." + _).mkString
    case ForeachUnion(x, e1, e2) =>
      s"""|For ${x.n} in ${quote(e1)} Union
          |${ind(quote(e2))}""".stripMargin
    case Union(e1, e2) =>
      s"(${quote(e1)}) Union (${quote(e2)})"
    case Singleton(e1) =>
      "sng(" + quote(e1) + ")"
    case Tuple(fields) =>
      s"( ${fields.map(f => f._1 + " := " + quote(f._2)).mkString(", ")} )"
    case Let(x, e1, e2) =>
      s"""|Let ${x.n} = ${quote(e1)} In
          |${ind(quote(e2))}""".stripMargin
    case Mult(e1, e2) =>
      s"Mult(${quote(e1)}, ${quote(e2)})"
    case IfThenElse(cond, e1, e2) => e2 match {
      case None => s"""|If (${cond.map(quote(_)).mkString(" & ")})
          |Then ${quote(e1)}""".stripMargin
      case Some(e3) => s"""|If (${cond.map(quote(_)).mkString(" & ")})
          |Then ${quote(e1)}
          |Else ${quote(e3)}""".stripMargin
    }
    case PhysicalBag(_, vs) => "[ " + vs.mkString(", ") + " ]"
    case Relation(n, _) => n
    case Label(vs, fe) =>
      "Label(vars := " + vs.map(quote).mkString(", ") + ", flat := " + quote(fe) + ")"
    case _ => throw new IllegalArgumentException("unknown type")
  }

  def quote(v: VarDef): String = v.n
  def quote(v: TupleVarDef): String = v.n
  def quote(v: BagVarDef): String = v.n
  def quote(c: Cond): String = s"(${quote(c.e1)} ${c.op} ${quote(c.e2)})" 
  def quote(c: Conditional): String = s" ${quote(c.e1)} ${c.op} ${quote(c.e2)} " 

  def quote(e: Calc): String = e match {
    case Constant(v, StringType) => "\""+ v +"\""
    case Constant(v, _) => v
    case v: Var => v.n + v.field.map("." + _).mkString
    case Sng(e1) =>
      "{" + quote(e1) + "}"
    case Zero() => "{ }"
    case Tup(fields) =>
      s"( ${fields.map(f => f._1 + " := " + quote(f._2)).mkString(", ")} )"
    case BagComp(e1, qs) => s"{ ${quote(e1)} | ${qs.map(quote(_)).mkString(", ")} }"
    case IfStmt(c, e1, e2) => e2 match {
      case Some(e3) => s"""|If (${c.map(quote(_)).mkString(" & ")})
          |Then ${quote(e1)}
          |Else ${quote(e3)}""".stripMargin
      case None => s"""|If (${c.map(quote(_)).mkString(" & ")})
          |Then ${quote(e1)}""".stripMargin
    }
    case Pred(c) => s"${quote(c)}"
    case Merge(e1, e2) => s"{ ${quote(e1)} U ${quote(e2)} }"
    case Bind(x, v) => s"${quote(x)} := ${quote(v)}"
    case Generator(x, v) => s" ${quote(x)} <- ${quote(v)} "
    case InputR(n, _) => n
    case _ => throw new IllegalArgumentException("unknown type")
  }

  def quote(e: AlgOp): String = e match {
    case Select(x, v, p @ Nil) => s"Select[lambda(${quote(v)}).true](${quote(x)})"
    case Select(x, v, p) => s"Select[lambda(${quote(v)}).${p.map(quote(_)).mkString(",")}](${quote(x)})"
    case Reduce(e1, v, e2 @ Nil) =>
      s"Reduce[ U / lambda(${v.map(quote(_)).mkString(",")}).${quote(e1)}, lambda(${v.map(quote(_)).mkString(",")}).true]"
    case Reduce(e1, v, e2) => 
      s"Reduce[ U / lambda(${v.map(quote(_)).mkString(",")}).${quote(e1)}, lambda(${v.map(quote(_)).mkString(",")}).${e2.map(quote(_)).mkString(",")}]"
    case Unnest(e1, e2, p @ Nil) => 
      s"Unnest[lambda(${e1.map(quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(quote(_)).mkString(",")}).true]"
    case Unnest(e1, e2, p) => 
      s"Unnest[lambda(${e1.map(quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
    case OuterUnnest(e1, e2, p @ Nil) => 
      s"OuterUnnest[lambda(${e1.map(quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(quote(_)).mkString(",")}).true]"
    case OuterUnnest(e1, e2, p) => 
      s"OuterUnnest[lambda(${e1.map(quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
    case Join(e1, p @ Nil) => s"Join[lambda(${e1.map(quote(_)).mkString(",")}).true]"
    case Join(e1, p) => s"Join[lambda(${e1.map(quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
    case OuterJoin(e1, p @ Nil) => s"OuterJoin[lambda(${e1.map(quote(_)).mkString(",")}).true]"
    case OuterJoin(e1, p) => s"OuterJoin[lambda(${e1.map(quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
    case Nest(e1, v, e2, p @ Nil, g) => 
      val w = v.map(quote(_)).mkString(",")
      val u = e2.map(quote(_)).mkString(",")
      val g2 = g.map(quote(_)).mkString(",")
      s"Nest[ U / lambda(${w}).${quote(e1)} / lambda(${w}).${u}, lambda(${w}).true / lambda(${w}).${g2}]" 
    case Nest(e1, v, e2, p, g) => 
      val w = v.map(quote(_)).mkString(",")
      val u = e2.map(quote(_)).mkString(",")
      val g2 = g.map(quote(_)).mkString(",")
      s"Nest[ U / lambda(${w}).${quote(e1)} / lambda(${w}).${u}, lambda(${w}).${p.map(quote(_)).mkString(",")} / lambda(${w}).${g2}]" 
    case Term(e1, e2 @ Init()) => s"${quote(e1)}"
    case Term(e1, e2) => s""" |${quote(e1)} 
                              |${ind(quote(e2))}""".stripMargin
    case Init() => ""
    case _ => throw new IllegalArgumentException("unknown type")
  }


}
