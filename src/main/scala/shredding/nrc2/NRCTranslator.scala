package shredding.nrc2

import shredding.core._
import shredding.calc._

/**
  * Translation functions for NRC
  * 
  */
trait NRCTranslator extends Calc{
  this: NRC =>

  object Translator extends Serializable{
    /**
      * Translate an NRC Expression into comprehension calculus.
      * Translation does some of the normalization rules 
      * simultaneously; however normalization is called 
      * at the end to ensure that all expressions are 
      * normalized. This makes it a bit easier to debug 
      * normalization and translation, if needed.
      * 
      */

    def translate(e: Expr): CompCalc = e match {
      case ForeachUnion(x, e1, e2) => translateBag(e2) match {
        case IfStmt(c, e3 @ Sng(t), e4 @ None) => BagComp(t, List(Generator(x, translateBag(e1)), c))
        case Sng(t) => BagComp(t, List(Generator(x, translateBag(e1))))
        case t =>
            val v = VarDef("v", t.tp.tp, VarCnt.inc)
            BagComp(TupleVar(v), List(Generator(x, translateBag(e1)), Generator(v, t))) 
        }
      case Union(e1, e2) => 
        Merge(translateBag(e1), translateBag(e2))
      case IfThenElse(cond, e1, e2) =>
        IfStmt(translateCond(cond), translateBag(e1), translateOption(e2))
      case Let(x, e1, e2) => e2.tp match {
        case t:BagType => 
          val te2 = translateBag(e2) 
          val v = VarDef("v", te2.tp.tp, VarCnt.inc) 
          BagComp(TupleVar(v), List(Bind(x, translate(e1)), Generator(v, te2)))
        case _ => sys.error("not supported")
      }
      case Singleton(e1) => Sng(translateTuple(e1))
      case Tuple(fs) => Tup(fs.map(x => x._1 -> translateAttr(x._2)))
      case p:Project => Proj(translateTuple(p.tuple), p.field)
      case Const(v, tp) => Constant(v, tp)
      case v:VarRef => Var(v.varDef)
      case Relation(n, b, t) => InputR(n, b, t)
      case NamedBag(n, b) => NamedCBag(n, translateBag(b))
      case Lookup(lbl, dict) => CLookup(translateLabel(lbl), dict.asInstanceOf[InputBagDict])
      case LabelRef(ld) => CLabelRef(ld) 
      case _ => sys.error("not supported")
    }

    def translate(e: VarDef) = e.tp match {
      case t:PrimitiveType => PrimitiveVar(e) 
      case t:BagType => BagVar(e)
      case t:TupleType => TupleVar(e)
      case t:LabelType => LabelVar(e)
    }

    // options are only used in if statements for now
    def translateOption(e: Option[Expr]): Option[BagCalc] = e match {
      case Some(e1) => Some(translateBag(e1))
      case _ => None
    }
    def translateBag(e: Expr): BagCalc = translate(e).asInstanceOf[BagCalc]
    def translateTuple(e: Expr): TupleCalc = translate(e).asInstanceOf[TupleCalc]
    def translateLabel(e: Expr): LabelCalc = translate(e).asInstanceOf[LabelCalc]
    def translateAttr(e: Expr): TupleAttributeCalc = translate(e).asInstanceOf[TupleAttributeCalc]
    def translateCond(e: Cond): Conditional = 
      Conditional(e.op, translateAttr(e.e1), translateAttr(e.e2)) 
  }

}
