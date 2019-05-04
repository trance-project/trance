package shredding.calc

import scala.collection.mutable.Map
import shredding.Utils.Symbol
import shredding.core._
import shredding.nrc.NRC

/**
  * Translation functions for NRC
  * 
  */
trait NRCTranslator extends NRC with Calc {

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

    val ctx: Map[String, Expr] = Map[String, Expr]()

    def translate(e: Expr): CompCalc = e match {
      case ForeachUnion(x, e1, e2) => 
      translateBag(e2) match {
        case IfStmt(c, e3 @ Sng(t), e4 @ None) => BagComp(t, List(Generator(x, translateBag(e1)), c))
        case Sng(t) => BagComp(t, List(Generator(x, translateBag(e1))))
        case t => 
          val v = VarDef(Symbol.fresh("v"), t.tp.tp)
          BagComp(TupleVar(v), List(Generator(x, translateBag(e1)), Generator(v, t))) 
        }
      case Union(e1, e2) => 
        Merge(translateBag(e1), translateBag(e2))
      case i: IfThenElse =>
        IfStmt(translateCond(i.cond), translateBag(i.e1), translateOption(i.e2))
      case l: Let => l.e2.tp match {
        case t:BagType => 
          val te2 = translateBag(l.e2)
          val v = VarDef(Symbol.fresh("v"), te2.tp.tp)
          BagComp(TupleVar(v), List(Bind(l.x, translate(l.e1)), Generator(v, te2)))
        case _ => sys.error("not supported")
      }
      case Singleton(e1) => Sng(translateTuple(e1))
      case Tuple(fs) => Tup(fs.map(x => x._1 -> translateAttr(x._2)))
      case p:Project => Proj(translateTuple(p.tuple), p.field)
      case Const(v, tp) => Constant(v, tp)
      case v:VarRef => Var(v.varDef)
      case _ => sys.error("not supported")
    }

    def translate(e: VarDef) = e.tp match {
      case t:PrimitiveType => PrimitiveVar(e) 
      case t:BagType => BagVar(e)
      case t:TupleType => TupleVar(e)
      case _ => sys.error("Cannot translate due to unknown type " + e.tp)
    }

    // options are only used in if statements for now
    def translateOption(e: Option[Expr]): Option[BagCalc] = e match {
      case Some(e1) => Some(translateBag(e1))
      case _ => None
    }
    def translateBag(e: Expr): BagCalc = translate(e).asInstanceOf[BagCalc]
    def translateTuple(e: Expr): TupleCalc = translate(e).asInstanceOf[TupleCalc]
    def translateVar(e: Expr): Var = translate(e).asInstanceOf[Var]
    def translateAttr(e: Expr): TupleAttributeCalc = translate(e).asInstanceOf[TupleAttributeCalc]
    def translateCond(e: Cond): Conditional = 
      Conditional(e.op, translateAttr(e.e1), translateAttr(e.e2)) 
  }

}
