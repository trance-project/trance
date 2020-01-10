package shredding.wmcc

import shredding.core._
import shredding.nrc.{LinearizedNRC, Printer => NRCPrinter}

/**
  * Translate (source and target) NRC to WMCC
  * Label nodes from NRC are represented as records, 
  * Free variables which are bound in a subquery from an extract node 
  * are bound by referencing and projecting on a label node
  */

trait NRCTranslator extends LinearizedNRC with NRCPrinter {
  val compiler = new BaseCompiler{}
  import compiler._

  def translate(e: Type): Type = e match {
    case BagType(t @ TupleType(fs)) if (fs.isEmpty) => BagCType(EmptyCType)
    case BagType(t @ TupleType(fs)) if (fs.keySet == Set("_1", "_2")) =>
      BagDictCType(BagCType(TTupleType(List(
        translate(fs.get("_1").get), translate(fs.get("_2").get)))), EmptyDictCType)
    case BagType(t) => BagCType(translate(t))
    case TupleType(fs) if fs.isEmpty => EmptyCType
    case TupleType(fs) => RecordCType(fs.map(f => f._1 -> translate(f._2)))
    case BagDictType(f, d) => f match {
      case BagType(t @ TupleType(fs)) if fs.keySet == Set("_1", "_2") =>
       BagDictCType(BagCType(TTupleType(List(
        translate(fs.get("_1").get), translate(fs.get("_2").get)))), 
          translate(d).asInstanceOf[TTupleDict])     
      case _ => 
       BagDictCType(BagCType(TTupleType(List(EmptyCType,translate(f).asInstanceOf[BagCType]))), 
        translate(d).asInstanceOf[TTupleDict])    
    }
    case EmptyDictType => EmptyDictCType
    case TupleDictType(ts) if ts.isEmpty => EmptyDictCType
    case TupleDictType(ts) => TupleDictCType(ts.map(f => f._1 -> translate(f._2).asInstanceOf[TDict]))
    case LabelType(fs) if fs.isEmpty => EmptyCType
    case LabelType(fs) => LabelType(fs.map(f => translateName(f._1) -> translate(f._2)))
    case _ => e
  }
  
  def translate(e: Cond): CExpr = e match {
    case Cmp(op, e1, e2) => op match {
      case OpEq => 
        compiler.equals(translate(e1), translate(e2))
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

  def translateName(name: String): String = name.replace("^", "__").replace("'", "").replace(".", "")
  def translate(v: VarDef): CExpr = Variable(translateName(v.name), translate(v.tp))
  def translateVar(v: VarRef): CExpr = v match {
    case BagVarRef(VarDef(_, BagType(TupleType(fs)))) => fs.get("lbl") match {
      case Some(LabelType(ms)) if ms.isEmpty => sng(record(Map("lbl" -> CUnit)))
      case _ => translate(v.varDef)
    }
    case _ => translate(v.varDef)
  }
  
  def translate(e: Expr): CExpr = e match {
    case Const(v, tp) => constant(v)
    case v:VarRef => translateVar(v)
    case PrimitiveOp(op, e1, e2) => mult(translate(e1), translate(e2))
    case Singleton(e1 @ Tuple(fs)) if fs.isEmpty => emptysng
    case Singleton(e1) => sng(translate(e1))
    case Tuple(fs) if fs.isEmpty => unit
    case Tuple(fs) => record(fs.map(f => f._2 match {
      // lifting alternating restriction in calculus
      //case Singleton(r @ Tuple(_)) => translateName(f._1) -> translate(r)
      case _ => translateName(f._1) -> translate(f._2)
    }))
    case p:Project => project(translate(p.tuple), p.field)
    case ift:IfThenElse => ift.e2 match {
      case Some(a) => ifthen(translate(ift.cond), translate(ift.e1), Option(translate(a)))
      case _ => ifthen(translate(ift.cond), translate(ift.e1))
    }
    case Union(e1, e2) => merge(translate(e1), translate(e2))
    case ForeachUnion(x, e1, e2) => translate(e2) match {
      case If(cond, e3, e4 @ None) => 
        Comprehension(translate(e1), translate(x).asInstanceOf[Variable], cond, e3)
      case te2 => 
        Comprehension(translate(e1), translate(x).asInstanceOf[Variable], constant(true), te2)
    }
    case l:Let => Bind(translate(l.x), translate(l.e1), translate(l.e2))
    case g:GroupBy => 
      CGroupBy(translate(g.bag), translate(g.v).asInstanceOf[Variable], translate(g.grp), translate(g.value))
    case Named(v, e) => e match {
      /**case Sequence(fs) => LinearCSet(fs.map{
        case Named(VarDef(n1, tp), e1) if n1 == "M_flat1" => CNamed(v.name+"__D_1", translate(e1))
        case nd => translate(nd)
      })**/
      case _ => CNamed(v.name, translate(e))
    }
    case Sequence(exprs) => 
      LinearCSet(exprs.map(translate(_)))
    case v:VarRefLabelParameter => translateVar(v.v)
    case l @ NewLabel(vs) => 
      record(vs.map(v => v match {
        case v2:VarRefLabelParameter => translateName(v2.name) -> translateVar(v2.v)
        case v2:ProjectLabelParameter => translateName(v2.name) -> translate(v2.p.asInstanceOf[Expr])
      }).toMap)
    case e:ExtractLabel =>  
      val lbl = translate(e.lbl)
      val bindings = e.lbl.tp.attrTps.map(k => 
        Variable(translateName(k._1), translate(k._2)) -> project(lbl, translateName(k._1))).toSeq
      bindings.foldRight(translate(e.e))((cur, acc) => Bind(cur._1, cur._2, acc))
    case Lookup(lbl, dict) => CLookup(translate(lbl), translate(dict)) 
    case EmptyDict => emptydict
    case BagDict(lbl, flat, dict) => BagCDict(translate(lbl), translate(flat), translate(dict))
    case BagDictProject(dict, field) => project(translate(dict), field)
    case TupleDict(fs) => TupleCDict(fs.map(f => f._1 -> translate(f._2)))
    case TupleDictProject(dict) => project(translate(dict), "_2")
    case DictUnion(d1, d2) => DictCUnion(translate(d1), translate(d2))
    case Total(e1) => comprehension(translate(e1), x => constant(true), (i: CExpr) => constant(1))
    case DeDup(e1) => CDeDup(translate(e1)) 
    case WeightedSingleton(tup, qty) => WeightedSng(translate(tup), translate(qty))
    case _ => EmptyCDict //sys.error("cannot translate "+e)
  }

}
