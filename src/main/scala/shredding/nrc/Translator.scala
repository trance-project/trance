package shredding.nrc

/**
  * Translate NRC Expression into Comprehension Calculus
  */

object Translator{

  /**
    * Translate an NRC Expression into comprehension calculus.
    * I've added the normalize flag, mainly false can be used for 
    * debugging.
    * 
    */
  def translate(e: Expr, normalize: Boolean = true): Calc = { 
    val calc = e match {
      case ForeachUnion(x, e1, e2) => qualifiers(translate(e2), List(Generator(x, translateBag(e1))))
      case Union(e1, e2) => Merge(translateBag(e1), translateBag(e2))
      case IfThenElse(cond, e1, e2) => e2 match {
        case Some(e3) => IfStmt(translateCond(cond), translateBag(e1), Some(translateBag(e3)))
        case None => qualifiers(translate(e1), List(translateCond(cond)))
      }
      case Let(x, e1, e2) => e2.tp match {
        case t:BagType => 
          val replaced = translate(e2).substitute(translate(e1), x)
          qualifiers(replaced, List(Bind(x, translate(e1))))
        case _ => 
          val replaced = translate(e2).substitute(translate(e1), x)
          Bind(VarDef("v", replaced.tp), replaced)
      }
      case Singleton(e1) => Sng(translateTuple(e1))
      case Tuple(fs) => Tup(fs.map(x => x._1 -> translateAttr(x._2)))
      case Const(v, tp) => Constant(v, tp)
      case PrimitiveVarRef(n, o, t) => PrimitiveVar(n, o, t)
      case BagVarRef(n, o, t) => BagVar(n, o, t)
      case TupleVarRef(n, o, t) => TupleVar(n, o, t)
      case Relation(n, b) => InputR(n, b)
      case _ => sys.error("not supported")
    }
    // return based on flag
    if (normalize) calc.normalize else calc
    
  }

  def qualifiers(e: Calc, qs: List[Calc]): Calc = e match {
    case BagComp(e1, q) => qualifiers(e1, qs ++ q)
    case Sng(e1) => BagComp(e1, qs)
    case Zero() => Zero()
    case _ => BagComp(e.asInstanceOf[TupleCalc], qs)
  }

  def translate(e: VarDef) = e match {
    case t:PrimitiveVarDef => PrimitiveVar(t.n, None, t.tp) 
    case t:BagVarDef => BagVar(t.n, None, t.tp)
    case t:TupleVarDef => TupleVar(t.n, None, t.tp)
  }

  def translateBag(e: Expr): BagCalc = translate(e).asInstanceOf[BagCalc]
  def translateTuple(e: Expr): TupleCalc = translate(e).asInstanceOf[TupleCalc]
  def translateAttr(e: Expr): AttributeCalc = translate(e).asInstanceOf[AttributeCalc]
  def translateCond(e: Cond): Conditional = 
    Conditional(e.op, translateAttr(e.e1), translateAttr(e.e2)) 

}
