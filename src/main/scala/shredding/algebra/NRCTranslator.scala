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
      BagDictCType(BagCType(TTupleType(List(LabelType(Map[String, Type]()), translate(f)))), 
        translate(d).asInstanceOf[TTupleDict])
    case EmptyDictType => EmptyDictCType
    case TupleDictType(ts) => TupleDictCType(ts.map(f => f._1 -> translate(f._2).asInstanceOf[TDict]))
    case LabelType(fs) => LabelType(fs.map(f => f._1.replace("^", "") -> translate(f._2)))
    case _ => e
  }
  
  def translate(e: Cond): CExpr = e match {
    case Cmp(op, e1, e2) => op match {
      case OpEq => compiler.equals(translate(e1), translate(e2))
      case OpNe => not(compiler.equals(translate(e1), translate(e2)))
      case OpGt => (translate(e1), translate(e2)) match {
        case (te1 @ Constant(_), te2:CExpr) =>  lt(te2, te1) // 5 > x
        case (te1:CExpr, te2:CExpr) => gt(te1, te2)
      }
      case OpGe => (translate(e1), translate(e2)) match {
        case (te1 @ Constant(_), te2:CExpr) => lte(te2, te1)
        case (te1:CExpr, te2:CExpr) => gte(te1, te2)
      }
    }
    case And(e1, e2) => and(translate(e1), translate(e2))
    case Or(e1, e2) => or(translate(e1), translate(e2))
    case Not(e1) => not(translate(e1))
  }

  def translate(v: VarDef): CExpr = Variable(v.name.replace("^", ""), translate(v.tp))
  def translateVar(v: VarRef): CExpr = translate(v.varDef)
 
  def translate(e: Expr): CExpr = e match {
    case Const(v, tp) => constant(v)
    case v:VarRef => translateVar(v)
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
        Comprehension(translate(e1), translate(x).asInstanceOf[Variable], cond, e3)
      case te2 => 
        Comprehension(translate(e1), translate(x).asInstanceOf[Variable], constant(true), te2)
    }
    case l:Let => Bind(translate(l.x), translate(l.e1), translate(l.e2))
    case Named(v, e) => CNamed(v.name, translate(e))
    case Sequence(exprs) => LinearCSet(exprs.map(translate(_)))
    // shredded
    case l @ NewLabel(vs) => 
      Label(l.id, vs.map( v => { 
        val v2 = translateVar(v).asInstanceOf[Variable]
        v2.name -> v2}).toList:_*)
    case e:ExtractLabel => //translate(e.lbl).asInstanceOf[Label].map( v => Bind(v._1, 
      Extract(translate(e.lbl), translate(e.e))
    case Lookup(lbl, dict) => CLookup(translate(lbl), translate(dict)) 
    case EmptyDict => emptydict
    case BagDict(lbl, flat, dict) => BagCDict(translate(lbl), translate(flat), translate(dict))
    case BagDictProject(dict, field) => project(translate(dict), field)
    case TupleDict(fs) => TupleCDict(fs.map(f => f._1 -> translate(f._2)))
    case TupleDictProject(dict) => project(translate(dict), "_2")
    case DictUnion(d1, d2) => DictCUnion(translate(d1), translate(d2))
    case Total(e1) => comprehension(translate(e1), x => constant(true), (i: CExpr) => constant(1))
    case DeDup(e1) => CDeDup(translate(e1)) 
  }

}
