package shredding.nrc

/**
  * Algebra.scala defines the algebraic operators
  * that are produced when a comprehension calculus expression
  * is normalized and unnested.
  *  
  */

sealed trait AlgOp { def tp: Type }

trait PrimitiveOutput extends AlgOp { def tp: PrimitiveType }
trait BagOutput extends AlgOp { def tp: BagType }
trait TupleOutput extends AlgOp { def tp: TupleType }

/**
  * Initial expression that will represent null input stream 
  * at the start of the unnesting algorithm 
  * ie. in unnesting rule C4: [{ e | v <- X, r, p }] {()}
  */
case class Init() extends BagOutput{
  def tp: BagType = BagType(TupleType())
}

/**
  * Select operator SELECT_p(X) = { v | v <- X, p(v) }
  * x: input relation
  * v: variable assigned to the tuple values coming from X
  * p: list of predicates associated to v only (no complex expressions)
  *    p(v) is the lambda expression in the subscript of the select operator
  *    the function is represented as a list of predicates, which should
  *    be evaluated as a conjunction
  *
  * With respect to unnesting:
  *   Select is always the first operator, u is always empty, and w is set
  *   to be equal to v
  */
case class Select(x: BagCalc, v: VarDef, p: List[Pred]) extends BagOutput{
  def tp: BagType = BagType(v.tp.asInstanceOf[TupleType])
}

/**
  * Reduce operator REDUCE_p^{acc/e}(X) = { e(v) | v <- X, p(v) }
  * v: variable assigned to the tuple values from from the input stream
  * e: lambda expression that should take v as input
  * p: list of predicates associated to v only (no complex expressions)
  *    p(v) is the lambda expression in the subscript of the reduce operator
  *    the function is represented as a list of predicates, which 
  *    should be evaluated as a conjunction
  *
  * With respect to unnesting:
  *   Reduce (project) is the last operator, u is always empty, and there should be
  *   no generators in the head of the comprehension.
  */
case class Reduce(e: Calc, v: List[VarDef], p: List[Pred]) extends AlgOp{
  def tp = e.tp
}

/**
  * Nest operator NEST_p/g^{acc/e/f}(X) = see fig 7
  * v: variables assigned to the tuple values from the input stream
  * e: lambda expression that should take v as input 
  * f: group by function, if two values are dot equal images under e are
  *    grouped together, then this group is reduced by the accumulator
  * p: list of predicates associated to v only (no complex expressions)
  * g: indicates which nulls to convert to zeros (lambda w. w/u = all variables in w
  *    that do not appear in u
  *
  * With respect to unnesting:
  *   Nest performs a group by operation and happens towards the end of the pipeline
  *   u and w are never empty if this condition is reached.
  *
  */
case class Nest(e: Calc, v: List[VarDef], f: List[VarDef], p: List[Pred], g: List[VarDef]) extends AlgOp{
  def tp = e.tp
}

/**
  * Unnest operator UNNEST_p^{path}(X) = { (v,w) | v <- X, w <- path(v), p(v,w) }
  * v: variable coming from the input stream
  * w: path information in the form a bag variable
  * p: list of predicates associated to the variable from the input stream (v) and w
  *    p(w,v) is the lambda expression in the subscript of the unnest operator 
  */
  // maybe v should be a list like reduce
case class Unnest(v: List[VarDef], w: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = w.tp
}

/**
  * Join oeprator X JOIN_p Y = { (v,w) | v <- X, w <- Y, p(v,w) }
  * v: variable coming from the input stream
  * w: generator associating source variable Y to the variable w (forms the select)
  * p: again a list of predicates representing the predicates of v and w 
  * 
  * With respect to unnesting:
  *   Unnesting a join produces two operators: an input stream from a select and a join to 
  *   accept that input stream. 
  *
  */
case class Join(v: List[VarDef],  p: List[Pred]) extends BagOutput{
  def tp: BagType = 
    BagType(TupleType(v.head.tp.asInstanceOf[TupleType].tps ++ v.tail.head.tp.asInstanceOf[TupleType].tps))
}

/**
  * OuterUnnest operator OUTER-UNNEST_p^{path}(X) = see fig7
  * v: variable coming from the input stream
  * w: path information in the form a bag variable that will later determine the 
  *    the logic for the outer-unnest
  * p: list of predicates associated to the variable from the input stream (v) and w
  *    p(w,v) is the lambda expression in the subscript of the outer-unnest operator 
  */
case class OuterUnnest(v: List[VarDef], w: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = w.tp
}

/**
  * OuterJoin oeprator X OUTER-JOIN_p Y = see fig7
  * v: variable coming from the input stream
  * w: generator associating source variable Y to the variable w (forms the select)
  *    that will later determine the logic for the outer-unnest
  * p: again a list of predicates representing the predicates of v and w 
  * 
  * With respect to unnesting:
  *   Unnesting a join produces two operators: an input stream from a select and a join to 
  *   accept that input stream. 
  *
  */
case class OuterJoin(v: List[VarDef], p: List[Pred]) extends BagOutput{
  def tp: BagType = 
    BagType(TupleType(v.head.tp.asInstanceOf[TupleType].tps ++ v.tail.head.tp.asInstanceOf[TupleType].tps))
}

/**
  * A term holds the expressions that are built up from the unnesting algorithm
  */
case class Term(e1: AlgOp, e2: AlgOp) extends AlgOp{
  def tp = e1.tp
}

