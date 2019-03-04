package shredding.nrc2

/**
  * Translate NRC Expression into Comprehension Calculus
  */

object Translator{

  /**
    * Translate an NRC Expression into comprehension calculus
    * this simultaneously transforms and normalizes
    */
  def translate(e: Expr): Calc = e match {
    case ForeachUnion(x, e1 @ Singleton(Tuple(ts)), e2) => ts.isEmpty match { 
      case true => Zero() // N5 
      case _ => qualifiers(translate(e2), List(Bind(x, translateTuple(e1)))) //N6
    }
    case ForeachUnion(x, e1 @ ForeachUnion(y, e3, e4), e2) => // N8 
      qualifiers(translate(e2), List(Generator(y, translateBag(e3)), 
                                     Bind(x, translateTuple(e4))))
    case ForeachUnion(x, e1, e2) => 
      qualifiers(translate(e2), List(Generator(x, translateBag(e1))))
    case Union(e1, e2) => Merge(translateBag(e1), translateBag(e2))
    case IfThenElse(cond, e1, e2) => e2 match {
      case Some(e3) => IfStmt(cond.map(translateCond(_)), translateBag(e1), Some(translateBag(e3)))
      case None => qualifiers(translate(e1), cond.map{ case c => Pred(translateCond(c))})
    }
    case Singleton(e1) => translate(e1) // N6
    // project on a tuple that isn't a tuplevar?
    case Tuple(fs) => Tup(fs.map(x => x._1 -> translateAttr(x._2)))
    case Const(v, tp) => Constant(v, tp)
    case PrimitiveVarRef(n, o, t) => PrimitiveVar(n, o, t)
    case BagVarRef(n, o, t) => BagVar(n, o, t)
    case TupleVarRef(n, o, t) => TupleVar(n, o, t)
    case Relation(n, b) => InputR(n, b)
    case _ => sys.error("not supported")
  }

  def qualifiers(e: Calc, qs: List[Calc]): Calc = e match {
    case BagComp(e1, q) => qualifiers(e1, qs ++ q)
    case Sng(e1) => BagComp(e1, qs)
    case Zero() => Zero()
    case _ => BagComp(e.asInstanceOf[TupleCalc], qs)
  }

  def translate(e: VarDef) = e match {
    case PrimitiveVarDef(n, tp) => PrimitiveVar(n, None, tp) 
    case BagVarDef(n, tp) => BagVar(n, None, tp)
    case TupleVarDef(n, tp) => TupleVar(n, None, tp)
  }

  def translateBag(e: Expr): BagCalc = translate(e).asInstanceOf[BagCalc]
  def translateTuple(e: Expr): TupleCalc = translate(e).asInstanceOf[TupleCalc]
  def translateAttr(e: Expr): AttributeCalc = translate(e).asInstanceOf[AttributeCalc]
  def translateCond(e: Cond): Conditional = Conditional(e.op, translateAttr(e.e1), translateAttr(e.e2)) 

}
