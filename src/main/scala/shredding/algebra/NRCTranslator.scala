package shredding.algebra

import shredding.core._
import shredding.nrc.LinearizedNRC

trait NRCTranslator extends LinearizedNRC {
  val compiler = new BaseCompiler{}
  import compiler._

  def translate(e: Type): Type = e match {
    case BagType(t @ TupleType(fs)) if fs.isEmpty => BagCType(EmptyCType)
    case BagType(t) => BagCType(translate(t))
    case TupleType(fs) if fs.isEmpty => EmptyCType
    case TupleType(fs) => RecordCType(fs.map(f => f._1 -> translate(f._2)))
    case BagDictType(f,d) => 
      BagDictCType(translate(f).asInstanceOf[BagCType], translate(d).asInstanceOf[TTupleDict])
    case EmptyDictType => EmptyDictCType
    case TupleDictType(ts) => TupleDictCType(ts.map(f => f._1 -> translate(f._2).asInstanceOf[TDict]))
    case LabelType(fs) => LabelType(fs.map(f => f._1 -> translate(f._2)))
    case _ => e
  }
  
  def translate(e: Cond): CExpr = e.op match {
    case OpEq => compiler.equals(translate(e.e1), translate(e.e2))
    case OpNe => not(compiler.equals(translate(e.e1), translate(e.e2)))
    case OpGt => (translate(e.e1), translate(e.e2)) match {
      case (te1 @ Constant(_), te2:CExpr) =>  lt(te2, te1) // 5 > x
      case (te1:CExpr, te2:CExpr) => gt(te1, te2)
    }
    case OpGe => (translate(e.e1), translate(e.e2)) match {
      case (te1 @ Constant(_), te2:CExpr) => lte(te2, te1)
      case (te1:CExpr, te2:CExpr) => gte(te1, te2)
    }
  }
 
  def translate(e: Expr): CExpr = e match {
    case Const(v, tp) => constant(v)
    case v:VarRef => Variable(v.varDef.name, translate(v.varDef.tp))
    case Singleton(e1 @ Tuple(fs)) if fs.isEmpty => emptysng
    case Singleton(e1) => sng(translate(e1))
    case Tuple(fs) if fs.isEmpty => unit
    case Tuple(fs) => record(fs.map(f => f._1 -> translate(f._2)))
    case p:Project => project(translate(p.tuple), p.field)
    case ift:IfThenElse => ift.e2 match {
      case Some(a) => ifthen(translate(ift.cond), translate(ift.e1), Option(translate(a)))
      case _ => ifthen(translate(ift.cond), translate(ift.e1))
    }
    case Union(e1, e2) => merge(translate(e1), translate(e2))
    case ForeachUnion(x, e1, e2) => translate(e2) match { 
      case If(cond, e3 @ Sng(t), e4 @ None) => 
        Comprehension(translate(e1), Variable(x.name, translate(x.tp)), cond, e3)
      case st @ Sng(t) => 
        Comprehension(translate(e1), Variable(x.name, translate(x.tp)), constant(true), st)
      case te2 => 
        Comprehension(translate(e1), Variable(x.name, translate(x.tp)), constant(true), te2)
    }
    case l:Let => Bind(Variable(l.x.name, translate(l.x.tp)), translate(l.e1), translate(l.e2))
    case Named(v, e) => CNamed(v.name, translate(e))
    case Sequence(exprs) => LinearCSet(exprs.map(translate(_)))
    // shredded
    case l @ NewLabel(vs) => 
      Label(l.id, vs.map( v => 
        translate(v.asInstanceOf[Expr]).asInstanceOf[Variable].name -> translate(v.asInstanceOf[Expr])).toList:_*)
    case Lookup(lbl, dict) => CLookup(translate(lbl), translate(dict)) 
    case EmptyDict => emptydict
    case BagDict(lbl, flat, dict) => BagCDict(translate(lbl), translate(flat), translate(dict))
    case BagDictProject(dict, field) => project(translate(dict), field)
    case TupleDict(fs) => TupleCDict(fs.map(f => f._1 -> translate(f._2)))
    case TupleDictProject(dict) => project(translate(dict), "tupleDict")
    case DictUnion(d1, d2) => DictCUnion(translate(d1), translate(d2))
    case Total(e) => comprehension(translate(e), x => constant(true), (i: CExpr) => constant(1))
  }

}
