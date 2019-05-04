package shredding.calc

import shredding.Utils.ind
import shredding.core._

trait AlgebraImplicits extends Algebra {

  implicit class AlgebraOps(self: AlgOp) {

    def quote: String = self match {
      case Select(x, v, p) => 
        s"Select[lambda(${core.quote(v)}).${p.quote}](${x.quote})"
      case Reduce(e1, v, p) =>
        s"Reduce[ U / lambda(${v.map(core.quote(_)).mkString(",")}).${e1.quote}, lambda(${v.map(core.quote(_)).mkString(",")}).${p.quote}]"
      case Unnest(e1, e2, p) =>
        s"Unnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${e2.quote}, lambda(${e1.map(core.quote(_)).mkString(",")}).${p.quote}]"
      case OuterUnnest(e1, e2, p) =>
        s"OuterUnnest[lambda(${e1.map(core.quote(_)).mkString(",")}).${e2.quote}, lambda(${e1.map(core.quote(_)).mkString(",")}).${p.quote}]"
      case Join(e1, p) => 
        s"Join[lambda(${e1.map(core.quote(_)).mkString(",")}).${p.quote}]"
      case OuterJoin(e1, p) => 
        s"OuterJoin[lambda(${e1.map(core.quote(_)).mkString(",")}).${p.quote}]"
      case Nest(e1, v, e2, p, g) =>
        val w = v.map(core.quote(_)).mkString(",")
        val u = e2.map(core.quote(_)).mkString(",")
        val g2 = g.map(core.quote(_)).mkString(",")
        val acc = e1.tp match {
          case t:PrimitiveType => "+"
          case _ => "U"
        }
        s"Nest[ ${acc} / lambda(${w}).${e1.quote} / lambda(${w}).${u}, lambda(${w}).${p.quote} / lambda(${w}).${g2}]"
      case Term(e1, e2 @ Init()) => s"${e1.quote}"
      case Term(e1, e2) => s""" |${e1.quote}
                              |${ind(e2.quote)}""".stripMargin
      case NamedTerm(n, t) => n+s" := ${t.quote}"
      case Init() => ""
      case _ => throw new IllegalArgumentException("unknown type")
    }

  }

}
