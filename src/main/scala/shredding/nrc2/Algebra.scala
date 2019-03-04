package shredding.nrc2

sealed trait AlgOp { def tp: Type }

// these are the possible output types of the algebra
trait PrimitiveOutput extends AlgOp { def tp: PrimitiveType }
trait BagOutput extends AlgOp { def tp: BagType }
trait TupleOutput extends AlgOp { def tp: TupleType }

//sealed trait Accumulator { def tp: Type }
//trait BagUnion extends Accumulator { def tp: BagType }

// this should be an input relation
case class Init() extends BagOutput{
  def tp: BagType = BagType(TupleType())
}
// x is a bag calc, but select only works on inputs?
// alg op wraps calc types to produce outputs 
case class Select(x: BagCalc, v: TupleVarDef, p: List[Pred]) extends BagOutput{
  def tp: BagType = BagType(v.tp)
}

// these types are wrong - need to merge (v, w)
case class Unnest(x: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = x.tp
}

case class Join(x: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = x.tp
}

// these types are wrong - need to merge (v, w)
case class OuterUnnest(x: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = x.tp
}

case class OuterJoin(x: BagCalc, p: List[Pred]) extends BagOutput{
  def tp: BagType = x.tp
}

// assume union accumulator
// make accumulator types?
case class Reduce(e: Calc, v: VarDef, p: List[Pred]) extends AlgOp{
  def tp = e.tp
}

case class Term2(e1: AlgOp, e2: AlgOp) extends AlgOp{
  def tp = e1.tp
}

//case class Join(x, y, ps) extends AlgOp
//case class Nest(x, path, ps) extends AlgOp
//case class Reduce(e, x, ps) extends AlgOp
