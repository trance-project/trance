package shredding.calc

import scala.collection.mutable.Map
import shredding.Utils.Symbol
import shredding.core._
import shredding.nrc.LinearizedNRC

/**
  * Translation functions for NRC
  * 
  */
trait NRCTranslator extends CalcImplicits with LinearizedNRC {

  implicit class NRCCondTranslators(e: Cmp){
    def translate: Conditional = 
      Conditional(e.op, e.e1.translate.asInstanceOf[TupleAttributeCalc], 
        e.e2.translate.asInstanceOf[TupleAttributeCalc]) 
  }

  implicit class NRCTranslators(self: Expr){

    def translate: CompCalc = self match {
      case ForeachUnion(x, e1, e2) => 
        e2.translate.asInstanceOf[BagCalc] match {
          case IfStmt(c, e3 @ Sng(t), e4 @ None) => BagComp(t, List(Generator(x, e1.translate.asInstanceOf[BagCalc]), c))
          case Sng(t) => BagComp(t, List(Generator(x, e1.translate.asInstanceOf[BagCalc])))
          case t => 
            val v = VarDef(Symbol.fresh("v"), t.tp.tp)
            BagComp(TupleVar(v), List(Generator(x, e1.translate.asInstanceOf[BagCalc]), Generator(v, t))) 
          }
      case Union(e1, e2) => 
        Merge(e1.translate.asInstanceOf[BagCalc], e2.translate.asInstanceOf[BagCalc])
      case i: IfThenElse =>
        val e2 = i.e2 match {
          case Some(b) => Option(b.translate.asInstanceOf[BagCalc])
          case None => None
        }
        IfStmt(i.cond.translate.asInstanceOf[PrimitiveCalc], i.e1.translate.asInstanceOf[BagCalc], e2)
      case l:Let => 
        val bind = BindDict(l.x, l.e1.translate)
        l.e2.tp match {
          case t:BagType =>
            val v = VarDef(Symbol.fresh("v"), t.tp)
            BagComp(TupleVar(v), List(bind, Generator(v, l.e2.translate.asInstanceOf[BagCalc])))
          case t:TupleDictType => 
            TupleDictComp(l.e2.translate.asInstanceOf[TupleDictCalc], bind.asInstanceOf[BindTupleDict])
        }
      case Singleton(e1) => Sng(e1.translate.asInstanceOf[TupleCalc])
      case Tuple(fs) => Tup(fs.map(x => x._1 -> x._2.translate.asInstanceOf[TupleAttributeCalc]))
      case p:Project => p.tp match {
        case t:LabelType => LabelProj(p.tuple.translate.asInstanceOf[TupleCalc], p.field)
        case _ => Proj(p.tuple.translate.asInstanceOf[TupleCalc], p.field)
      }
      case Const(v, tp) => Constant(v, tp)
      case Total(b) => 
        val v = VarDef(Symbol.fresh("v"), b.tp.tp)
        CountComp(Constant(1, IntType), List(Generator(v, b.translate.asInstanceOf[BagCalc])))
      case v:VarRef => v.tp match {
        case t:LabelType => LabelVar(v.varDef)
        case t:DictType => DictVar(v.varDef)
        case _ => Var(v.varDef)
      }
      case l @ NewLabel(vs) => CLabel(l.id, vs.map{ case v:VarRef =>
        v.varDef.name -> v.asInstanceOf[Expr].translate}.toList:_*)
      case Lookup(lbl, dict) => CLookup(lbl.translate.asInstanceOf[LabelCalc], 
        dict.translate.asInstanceOf[BagDictCalc])
      // dict union 
      case DictUnion(d1, d2) => 
        DictCUnion(d1.translate.asInstanceOf[DictCalc], d2.translate.asInstanceOf[DictCalc])
      case TupleDictProject(dict) => TupleDictProj(dict.translate.asInstanceOf[BagDictCalc])
      case BagDictProject(dict, field) => BagDictProj(dict.translate.asInstanceOf[TupleDictCalc], field)
      case TupleDict(fs) => 
        TupleCDict(fs.map(f => f._1 -> f._2.translate.asInstanceOf[TupleDictAttributeCalc]))
      case BagDict(lbl, flat, dict) =>
        BagCDict(lbl.translate.asInstanceOf[LabelCalc], flat.translate.asInstanceOf[BagCalc], 
          dict.translate.asInstanceOf[TupleDictCalc])
      case EmptyDict => EmptyCDict
      case Sequence(exprs) => 
        exprs.foreach(expr => println(expr.translate.quote))
        CSequence(exprs.map(_.translate))
      case Named(v, e) => CNamed(v, e.translate)
      case _ => sys.error("not supported "+self)
    }

    // options are only used in if statements for now
    def translateOption(e: Option[Expr]): Option[BagCalc] = e match {
      case Some(e1) => Some(translateBag(e1))
      case _ => None
    }
    def translateBag(e: Expr): BagCalc = e.translate.asInstanceOf[BagCalc]
    def translateTuple(e: Expr): TupleCalc = e.translate.asInstanceOf[TupleCalc]
    def translateAttr(e: Expr): TupleAttributeCalc = e.translate.asInstanceOf[TupleAttributeCalc]
    def translateCond(e: Cond): PrimitiveCalc = e match {
      case Cmp(op, e1, e2) =>
        Conditional(op, translateAttr(e1), translateAttr(e2))
      case And(e1, e2) =>
        AndCondition(translateCond(e1), translateCond(e2))
      case Or(e1, e2) =>
        OrCondition(translateCond(e1), translateCond(e2))
      case Not(e1) =>
        NotCondition(translateCond(e1))
    }

  }


}
