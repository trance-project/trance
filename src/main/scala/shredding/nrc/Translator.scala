package shredding.nrc

/**
  * Translate NRC Expression into Comprehension Calculus
  * 
  */

object Translator{

  var normalize = true
  /**
    * Translate an NRC Expression into comprehension calculus.
    * Translation does some of the normalization rules 
    * simultaneously; however normalization is called 
    * at the end to ensure that all expressions are 
    * normalized. This makes it a bit easier to debug 
    * normalization and translation, if needed.
    * 
    */
  def translate(e: Expr): Calc = { 
    val calc = e match {
      case ForeachUnion(x, e1, e2) => 
        qualifiers(translate(e2), List(Generator(x, translateBag(e1))))
      case Union(e1, e2) => Merge(translateBag(e1), translateBag(e2))
      case IfThenElse(cond, e1, e2) => 
        IfStmt(translateCond(cond), translateBag(e1), translateOption(e2))
      case Let(x, e1, e2) => e2.tp match {
        case t:BagType => // { e3 | x := e1, ... }
          val replaced = translate(e2).substitute(translate(e1), x)
          qualifiers(replaced, List(Bind(x, translate(e1))))
        case _ => 
          // substitutes all x for e1 values
          // does tuple normalization (projection)
          // and creates a new variable for the newly created value
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
      case TotalMult(e1) => 
        val qtc = translateBag(e1)
        qtc match {
          case BagComp(e, qs) => CountComp(Constant("1", IntType), qs)
          case _ => CountComp(Constant("1", IntType), List(qtc))
        }
      case _ => sys.error("not supported")
    }
    
    if (normalize) calc.normalize else calc
    
  }

  /**
    * Qualifiers is used for transformation an expression with 
    * nesting in the head (Limsoon style) into a sequential list of 
    * qualifiers in the body as in F&M
    *
    * For example:
    *   Source input: For x in R For y in x.a if y = x.b then sng(y)
    *   transformed with recursion via: { translate( For y in x.a if y = x.b then sng(y) ) | x <- translate(R) }
    *   would eventually produce: { { { y | y = x.b } | y <- x.a } | x <- R } (limsoon style)
    *   the qualifiers function turns the head comprehensions 
    *   into a series of qualifiers: { y | x <- R, y <- x.a, y = x.b }
    * 
    * @param e Calc expression that is in the head of the original bag comprehension
    * @param qs qualifiers to append new qualifiers brought over from head
    * 
    * @returns BagCalc bag comprehension with appropriately ordered qualifiers in the body (or Zero)
    */
  def qualifiers(e: Calc, qs: List[Calc]): BagCalc = e match {
    case BagComp(e1, q) => qualifiers(e1, qs ++ q)
    case Sng(e1) => BagComp(e1, qs)
    case Zero() => Zero()
    // basic predicate qualifier
    // for x in R if x < 2 then sng(x.a) => 
    // { x.a | x <- R, x < 2 }
    case IfStmt(cond, e1, e2 @ None) => qualifiers(e1, qs :+ cond)
    // for x in R if x < 2 then sng(x.a) else sng (x.b) =>
    // { x.a | x <- R, x < 2 } U { x.b | x <- R, not( x < 2 ) } 
    case IfStmt(cond, e1, e2 @ Some(e3)) => 
      Merge(qualifiers(e1, qs :+ cond), qualifiers(e3, qs :+ NotCondition(cond)))
    case Merge(e1, e2) => 
      Merge(qualifiers(e1, qs), qualifiers(e2, qs))
    case _ => BagComp(e.asInstanceOf[TupleCalc], qs)
  }

  def translate(e: VarDef) = e match {
    case t:PrimitiveVarDef => PrimitiveVar(t.n, None, t.tp) 
    case t:BagVarDef => BagVar(t.n, None, t.tp)
    case t:TupleVarDef => TupleVar(t.n, None, t.tp)
  }

  // options are only used in if statements for now
  def translateOption(e: Option[Expr]): Option[BagCalc] = e match {
    case Some(e1) => Some(translateBag(e1))
    case _ => None
  }
  def translateBag(e: Expr): BagCalc = translate(e).asInstanceOf[BagCalc]
  def translateTuple(e: Expr): TupleCalc = translate(e).asInstanceOf[TupleCalc]
  def translateAttr(e: Expr): AttributeCalc = translate(e).asInstanceOf[AttributeCalc]
  def translateCond(e: Cond): Conditional = 
    Conditional(e.op, translateAttr(e.e1), translateAttr(e.e2)) 

}
