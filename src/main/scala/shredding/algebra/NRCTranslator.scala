package shredding.algebra

import shredding.core._
import shredding.nrc.LinearizedNRC

trait NRCTranslator extends LinearizedNRC {
  type Rep = CExpr
  
  val compiler = new BaseCompiler{}
  import compiler._

  def translate(e: Cond): Rep = e.op match {
    case OpEq => compiler.equals(translate(e.e1), translate(e.e2))
    case OpNe => not(compiler.equals(translate(e.e1), translate(e.e2)))
    case OpGt => (translate(e.e1), translate(e.e2)) match {
      case (te1 @ Constant(_), te2:Rep) =>  lt(te2, te1) // 5 > x
      case (te1:Rep, te2:Rep) => gt(te1, te2)
    }
    case OpGe => (translate(e.e1), translate(e.e2)) match {
      case (te1 @ Constant(_), te2:Rep) => lte(te2, te1)
      case (te1:Rep, te2:Rep) => gte(te1, te2)
    }
  }

  def translate(e: Expr): Rep = e match {
    case Const(v, tp) => constant(v)
    case v:VarRef => Variable(v.varDef.name, v.varDef.tp)
    case Singleton(e1) => sng(translate(e1))
    case Tuple(fs) => tuple(fs.map(f => f._1 -> translate(f._2)))
    case p:Project => project(translate(p.tuple), p.field)
    case ift:IfThenElse => ift.e2 match {
      case Some(a) => ifthen(translate(ift.cond), translate(ift.e1), Option(translate(a)))
      case _ => ifthen(translate(ift.cond), translate(ift.e1))
    }
    case Union(e1, e2) => merge(translate(e1), translate(e2))
    case ForeachUnion(x, e1, e2) => 
      Comprehension(translate(e1), Variable(x.name, x.tp), constant(true), translate(e2))
    case l:Let =>  Bind(Variable(l.x.name, l.x.tp), translate(l.e1), translate(l.e2))
    case Total(e) => comprehension(translate(e), x => constant(true), (i: Rep) => constant(1))
  }

}
