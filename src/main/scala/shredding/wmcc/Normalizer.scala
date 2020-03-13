package shredding.wmcc

import shredding.core._

/**
  * Normalization rules for WMCC 
  */

trait BaseNormalizer extends BaseCompiler {

  // reduce conditionals
  override def equals(e1: Rep, e2: Rep): Rep = (e1, e2) match {
    case (Constant(x1), Constant(x2)) => constant(x1 == x2)
    case _ => super.equals(e1, e2)
  }

  override def not(e1: Rep): Rep = e1 match {
    case Constant(true) => super.constant(false)
    case Constant(false) => super.constant(true)
    case _ => super.not(e1)
  }

  override def and(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(true), Constant(true)) => super.constant(true)
    case (Constant(false), _) => super.constant(false)
    case (_, Constant(false)) => super.constant(false)
    case (Constant(true), e3) => e3
    case (e3, Constant(true)) => e3
    case _ => super.and(e1, e2)
  }

  override def or(e1: Rep, e2: Rep): Rep  = (e1, e2) match {
    case (Constant(false), Constant(false)) => super.constant(false)
    case (Constant(true), _) => super.constant(true)
    case (_, Constant(true)) => super.constant(true)
    case _ => super.or(e1, e2)
  }

  override def lookup(lbl: Rep, dict: Rep): Rep = dict match {
    case BagCDict(lblTp2, flat2, dict2) if lblTp2 == lbl.tp => flat2
    case BagCDict(lblTp2, flat2, dict2) =>///???
      super.lookup(lbl, dict)
    case _ => 
      super.lookup(lbl, dict)
  }
  
  /** need to update the types in a sequence **/
  override def linset(es: List[Rep]): Rep = {
    LinearCSet(es.flatMap{
      case LinearCSet(fs) => fs
      case fs => List(fs)
    })
  } 

  // N1, N2
  override def bind(e1: Rep, e: Rep => Rep): Rep = e(e1)

  // N3 (a := e1, b: = e2).a = e1
  override def project(e1: Rep, f: String): Rep = e1 match {
    case t:Tuple => t(f.toInt)
    case t:Record => t(f)
    //case t:Label => t(f)
    case t:TupleCDict => t(f)
    case t:BagCDict => t(f)
    case _ => super.project(e1, f)
  }

  override def ifthen(cond: Rep, e1: Rep, e2: Option[Rep]): Rep = cond match {
    case Constant(true) => e1 match {
      case Sng(t) if t.tp.isInstanceOf[PrimitiveType] => t
      case _ => e1
    }
    case Constant(false) => e2 match {
      case Some(a @ Sng(t)) if t.tp.isInstanceOf[PrimitiveType] => t 
      case Some(a) => a 
      case _ => super.emptysng
    }
    case _ => e1 match {
      case Sng(t) if t.tp == IntType => super.ifthen(cond, t, Some(Constant(0)))
      case Sng(t) if t.tp == DoubleType => super.ifthen(cond, t, Some(Constant(0.0))) 
      case If(cond2, e3, e4) => e2 match {
        case None => ifthen(and(cond, cond2), e3, e4)
        case Some(e5) => ifthen(and(cond, not(cond2)), e4.get, Some(ifthen(and(cond, cond2), e3, e2)))
      }
      case _ => super.ifthen(cond, e1, e2)
    }
  }

  override def sng(x: Rep): Rep = x match {
    case Record(fs) => fs.get("_1") match {
      case Some(CUnit) => fs.get("_2").get 
      case _ => super.sng(x)
    }
    case Sng(t) => sng(t)
    case y if x.tp.isInstanceOf[BagCType] => x
    case _ => super.sng(x)
  }
 
  // { e(v) | v <- e1, p(v) }
  // where fegaras and maier does: { e | q, v <- e1, s }
  // this has { { { e | s } | v <- e1 } | q }
  // the normalization rules reduce generators, which is why I match on e1
  // N10 is automatically handled in this representation
  override def comprehension(e1: Rep, p: Rep => Rep, e: Rep => Rep): Rep = {
    e1 match {
      case If(cond, e3, e4 @ Some(a)) => //N4
        If(cond, comprehension(e3, p, e), Some(comprehension(a, p, e)))
      case If(cond, e3 @ WeightedSng(t, q), None) => comprehension(e3, (i: CExpr) => cond, e)
      case WeightedSng(t, q) if e(t) == Constant(1) => comprehension(Sng(t), p, (i: CExpr) => q)
      case EmptySng => EmptySng // N5
      case Sng(t) => ifthen(p(t), sng(e(t))) // N6
      case Merge(e1, e2) => Merge(comprehension(e1, p, e), comprehension(e2, p, e))  //N7
      case Variable(name,_) => ifthen(p(e1), Sng(e(e1))) // input relation
      case Comprehension(e2, v2, p2, e3) => e3 match {
        // weighted singleton used for count
        // { 1 | v <- { WeightedSng(t,q) | v2 <- e2, p2}, p(v) }
        // { if (p(v)) { q } else { 0 } | v2 <- e2, p2, v := t, ... }
        // need to sum if more than one weighted sng is in a comprehension
        case WeightedSng(t, q) if (e(e3) == Constant(1)) =>
          val c2 = comprehension(Sng(t), p, (i: CExpr) => q) match {
            case Comprehension(a,b,c,Constant(1)) => Comprehension(a, b, c, q)
            case c3 => c3
          } 
          Comprehension(e2, v2, p2, c2)
        //N8
        // { e(v) | v <- { e3 | v2 <- e2, p2 }, p(v) }
        // { { e(v) | v <- e3 } | v2 <- e2, p2 }
        case _ =>
          comprehension(e3, p, e) match {
            case If(cond, e4, None) => Comprehension(e2, v2, and(p2, cond), e4)
            case c => Comprehension(e2, v2, p2, c)
          }
      }
      case c @ CLookup(flat, dict) => 
        val v = dict.tp match {
          case BagCType(tup) => Variable.fresh(tup)
          case BagDictCType(BagCType(TTupleType(List(EmptyCType, BagCType(tup)))), tdict) =>
            Variable.fresh(tup)
          case _ => Variable.fresh(dict.tp.asInstanceOf[BagDictCType].flat.tp)
        }
        flat.tp match {
          /**case EmptyCType => 
            Comprehension(Project(dict, "_1"), v, p(v), e(v)) **/
          case _ =>
            Comprehension(e1, v, p(v), e(v))
        }
      // this will break this for the shredded case...
      case InputRef(n, tp) if isTopLevelDict(tp) =>
        val v = topLevelVar(tp)
        e(v) match {
          // top level dictionary case from unshredding
          case Comprehension(Project(_,"_2"), v2, p2, Sng(Record(fs))) if fs.keySet == Set("_1", "_2") =>
            Comprehension(InputRef(n, tp), v2, p2, fs.get("_2").get)  
          case Comprehension(Project(_,"_2"), v2, p2, e3) =>
            Comprehension(InputRef(n, tp), v2, p2, e3)  
          case _ => ???
        }
       case _ => // standard case (return self)
        val v = e1.tp match {
          case btp @ BagDictCType(BagCType(TTupleType(fs)),tdict) => Variable.fresh(btp._1.tp)
          case _ => Variable.fresh(e1.tp.asInstanceOf[BagCType].tp)
        }
        Comprehension(e1, v, p(v), e(v))
      }
    }

  def isTopLevelDict(tp: Type): Boolean = tp match {
    case BagDictCType(BagCType(RecordCType(fs)), EmptyDictCType) => true
    case BagCType(RecordCType(fs)) if fs.keySet == Set("lbl") => false
    case BagCType(RecordCType(fs)) if fs.exists(_._2.isInstanceOf[LabelType]) => true
    case _ => false
  }

  def topLevelVar(tp: Type): Variable = tp match {
    case BagDictCType(BagCType(RecordCType(fs)), EmptyDictCType) => 
      Variable.fresh(TTupleType(List(EmptyCType,tp)))
    case BagCType(rs @ RecordCType(fs)) if fs.exists(_._2.isInstanceOf[LabelType]) => 
      Variable.fresh(TTupleType(List(EmptyCType, tp)))
    case _ => ???
  }

}
