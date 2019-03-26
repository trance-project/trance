package shredding.calc

import shredding.Utils.ind
import shredding.core._

package object calc extends Calc with Algebra { 

    def quote(e: CompCalc): String = e match {
      case Constant(v, _) => "\""+ v +"\""
      case v: Var => v.name
      case Sng(e1) =>
        "{" + quote(e1) + "}"
      case Zero() => "{ }"
      case Tup(fields) =>
        s"( ${fields.map(f => f._1 + " := " + quote(f._2)).mkString(", ")} )"
      case p:Proj => quote(p.tuple)+"."+p.field
      case BagComp(e1, qs) => s"{ ${quote(e1)} | ${qs.map(quote(_)).mkString(", ")} }"
      case IfStmt(c, e1, e2) => e2 match {
        case Some(e3) => s"""|If (${quote(c)})
          |Then ${quote(e1)}
          |Else ${quote(e3)}""".stripMargin
        case None => s"""|If (${quote(c)})
          |Then ${quote(e1)}""".stripMargin
      }
      case Conditional(op, e1, e2) => s" ${quote(e1)} ${op} ${quote(e2)} "
      case NotCondition(e1) => s" not(${quote(e1)}) "
      case AndCondition(e1, e2) => s" ${quote(e1)} and ${quote(e2)} "
      case OrCondition(e1, e2) => s" ${quote(e1)} or ${quote(e2)} "
      case Merge(e1, e2) => s"{ ${quote(e1)} U ${quote(e2)} }"
      case BindPrimitive(x, v) => s"${core.quote(x)} := ${quote(v)}"
      case BindTuple(x, v) => s"${core.quote(x)} := ${quote(v)}"
      case Generator(x, v) => s" ${core.quote(x)} <- ${quote(v)} "
      case InputR(n, _, _) => n
      case CountComp(e1, qs) => s" + { ${quote(e1)} | ${qs.map(quote(_)).mkString(",")} }"
      case NamedCBag(n, b) => n+s" := ${quote(b)} "
      case CLabelRef(ld) => ld.toString
      case CLookup(lbl, dict) => s"Lookup(${dict.toString})(${quote(lbl)})"
      case _ => throw new IllegalArgumentException("unknown type")
    }

    def quote(e: AlgOp): String = e match {
      case Select(x, v, p @ Nil) => s"Select[lambda(${core.quote(v)}).true](${quote(x)})"
      case Select(x, v, p) => s"Select[lambda(${core.quote(v)}).${p.map(quote(_)).mkString(",")}](${quote(x)})"
      case Reduce(e1, v, e2 @ Nil) =>
        s"Reduce[ U / lambda(${v.map(core.quote(_)).mkString(",")}).${quote(e1)}, lambda(${v.map(core.quote(_)).mkString(",")}).true]"
      case Reduce(e1, v, e2) =>
        s"Reduce[ U / lambda(${v.map(core.quote(_)).mkString(",")}).${quote(e1)}, lambda(${v.map(core.quote(_)).mkString(",")}).${e2.map(quote(_)).mkString(",")}]"
      case Unnest(e1, e2, p @ Nil) =>
        s"Unnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(core.quote(_)).mkString(",")}).true]"
      case Unnest(e1, e2, p) =>
        s"Unnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(core.quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
        case OuterUnnest(e1, e2, p @ Nil) =>
        s"OuterUnnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(core.quote(_)).mkString(",")}).true]"
      case OuterUnnest(e1, e2, p) =>
        s"OuterUnnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${quote(e2)}, lambda(${e1.map(core.quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
      case Join(e1, p @ Nil) => s"Join[lambda(${e1.map(core.quote(_)).mkString(",")}).true]"
      case Join(e1, p) => s"Join[lambda(${e1.map(core.quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
      case OuterJoin(e1, p @ Nil) => s"OuterJoin[lambda(${e1.map(core.quote(_)).mkString(",")}).true]"
      case OuterJoin(e1, p) => s"OuterJoin[lambda(${e1.map(core.quote(_)).mkString(",")}).${p.map(quote(_)).mkString(",")}]"
      case Nest(e1, v, e2, p @ Nil, g) =>
        val w = v.map(core.quote(_)).mkString(",")
        val u = e2.map(core.quote(_)).mkString(",")
        val g2 = g.map(core.quote(_)).mkString(",")
        s"Nest[ U / lambda(${w}).${quote(e1)} / lambda(${w}).${u}, lambda(${w}).true / lambda(${w}).${g2}]"
      case Nest(e1, v, e2, p, g) =>
        val w = v.map(core.quote(_)).mkString(",")
        val u = e2.map(core.quote(_)).mkString(",")
        val g2 = g.map(core.quote(_)).mkString(",")
        s"Nest[ U / lambda(${w}).${quote(e1)} / lambda(${w}).${u}, lambda(${w}).${p.map(quote(_)).mkString(",")} / lambda(${w}).${g2}]"
      case Term(e1, e2 @ Init()) => s"${quote(e1)}"
      case Term(e1, e2) => s""" |${quote(e1)}
                              |${ind(quote(e2))}""".stripMargin
      case NamedTerm(n, t) => n+s" := ${quote(t)}"
      case Init() => ""
      case _ => throw new IllegalArgumentException("unknown type")
    }
  
}
