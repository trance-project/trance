package shredding.algebra

import shredding.core._
import shredding.algebra.{Comprehension => CComp, Bind => CBind, Merge => CMerge, Sng => CSng, Constant => CConstant}
import shredding.calc.CalcImplicits


trait BackTranslator extends CalcImplicits {
  val compiler = new BaseCompiler{}
  import compiler._

  def translate(e: Type): Type = e match {
    case BagCType(EmptyCType) => BagType(TupleType(Map[String, TupleAttributeType]()))
    case BagCType(t) => BagType(translate(t).asInstanceOf[TupleType])
    case EmptyCType => TupleType(Map[String, TupleAttributeType]())
    case RecordCType(fs) => TupleType(fs.map(f => f._1 -> translate(f._2).asInstanceOf[TupleAttributeType]))
    case _ => e
  }

  def translate(e: CExpr): CompCalc = e match {
    case Equals(e1, e2) => Conditional(OpEq, translate(e1), translate(e2))
    case Not(e1) => NotCondition(translate(e1))
    case Gt(e1, e2) => Conditional(OpGt, translate(e1), translate(e2))
    case Lt(e1, e2) => Conditional(OpGt, translate(e2), translate(e1))
    case Gte(e1, e2) => Conditional(OpGe, translate(e1), translate(e2))
    case Lte(e1, e2) => Conditional(OpGe, translate(e2), translate(e1))
    case CConstant(x) => Constant(x, e.tp.asInstanceOf[PrimitiveType])
    case Variable(n, tp) => Var(VarDef(n, translate(tp)))
    case EmptySng => Sng(Tup(Map[String, TupleAttributeCalc]()))
    case CSng(e1) => Sng(translate(e1).asInstanceOf[TupleCalc])
    case CUnit => Tup(Map[String, TupleAttributeCalc]())
    //case Tuple(fs)  =>  Tup(fs.zipWithIndex.map(f => f._2 -> translate(f._1).asInstanceOf[TupleAttributeCalc]))
    case Record(fs) => Tup(fs.map(f => f._1 -> translate(f._2).asInstanceOf[TupleAttributeCalc]))
    case Project(e, f) => Proj(translate(e).asInstanceOf[TupleCalc], f)
    case If(cond, e1, e2) => e2 match {
      case Some(a) => 
        IfStmt(translate(cond).asInstanceOf[PrimitiveCalc], translate(e1).asInstanceOf[BagCalc], 
          Some(translate(a).asInstanceOf[BagCalc]))
      case _ => IfStmt(translate(cond).asInstanceOf[PrimitiveCalc], translate(e1).asInstanceOf[BagCalc]) 
    } 
    case CMerge(e1, e2) => Merge(translate(e1).asInstanceOf[BagCalc], translate(e2).asInstanceOf[BagCalc])
    case CComp(e1, v, p, m) => translate(m) match {
      case c:Comprehension => 
        Comprehension(c.e, List(Generator(translate(v).asInstanceOf[Var].varDef, translate(e1).asInstanceOf[BagCalc]),
                                translate(p)) ++ c.qs)
      case st @ Sng(t) => 
        Comprehension(t, List(Generator(translate(v).asInstanceOf[Var].varDef, 
          translate(e1).asInstanceOf[BagCalc]), translate(p)))
      case t => 
        Comprehension(t, List(Generator(translate(v).asInstanceOf[Var].varDef, 
          translate(e1).asInstanceOf[BagCalc]), translate(p)))
    }
    case CBind(x, e1, e) => translate(e).bind(translate(e1), translate(x).asInstanceOf[Var].varDef)
  }

}
