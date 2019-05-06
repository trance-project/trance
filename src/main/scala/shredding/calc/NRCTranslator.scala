package shredding.calc

import scala.collection.mutable.Map
import shredding.Utils.Symbol
import shredding.core._
import shredding.nrc.LinearizedNRC

/**
  * Translation functions for NRC
  * 
  */
trait NRCTranslator extends ShredCalc with LinearizedNRC {

  implicit class NRCCondTranslators(e: Cond){
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
        IfStmt(i.cond.translate, i.e1.translate.asInstanceOf[BagCalc], e2)
      case l: Let => l.e2.tp match {
        case t:BagType => 
          val te2 = l.e2.translate.asInstanceOf[BagCalc]
          val v = VarDef(Symbol.fresh("v"), te2.tp.tp)
          val qs = l.e1.tp match {
            case t:DictType => 
              List(BindDict(l.x, l.e1.translate.asInstanceOf[DictCalc]), Generator(v, te2))
            case _ => List(Bind(l.x, l.e1.translate), Generator(v, te2))
          }
          BagComp(TupleVar(v), qs)
        case _ => sys.error("todo need to support other lets "+self+" "+l.e2.tp)
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
      case Sequence(exprs) => CSequence(exprs.map(_.translate))
      case Named(v, e) => CNamed(v, e.translate)
      case _ => sys.error("not supported "+self)
    }

  }


}
