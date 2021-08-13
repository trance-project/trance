package framework.plans

import framework.common._

/** Extensions for the plan language **/
trait Extensions {

  def replace(e: CExpr, v: Variable): CExpr = e match {
    case Record(ms) => Record(ms.map(r => r._1 -> replace(r._2, v)))
    case Project(_, f) => Project(v, f)
    case If(cond, s1, Some(s2)) => If(replace(cond, v), replace(s1, v), Some(replace(s2, v)))
    case If(cond, s1, None) => If(replace(cond, v), replace(s1, v), None)
    case Equals(e1, e2) => Equals(replace(e1, v), replace(e2, v))
    case MathOp(op, e1, e2) => MathOp(op, replace(e1, v), replace(e2, v))
    // this will be additional base ops
    case _:Variable => v
    case _ => e
  }

  // todo additional cases
  def collect(e: CExpr): Set[String] = e match {
    case Record(e1) => e1.flatMap(f => collect(f._2)).toSet
    case Label(e1) => e1.flatMap(f => collect(f._2)).toSet
    case If(cond, s1, Some(s2)) => collect(cond) ++ collect(s1) ++ collect(s2)
    case If(cond, s1, None) => collect(cond) ++ collect(s1)
    case MathOp(op, e1, e2) => collect(e1) ++ collect(e2)
    case Equals(e1, e2) => collect(e1) ++ collect(e2)
    case Not(e1) => collect(e1)
    case And(e1, e2) => collect(e1) ++ collect(e2)
    case Gte(e1, e2) => collect(e1) ++ collect(e2)
    case Lte(e1, e2) => collect(e1) ++ collect(e2)
    case Project(e1, f) => Set(f)
  	case CUdf(n, e1, tp) => collect(e1)
  	case _ => Set()
  }

  def fapply(e: CExpr, funct: PartialFunction[CExpr, CExpr]): CExpr = 
    funct.applyOrElse(e, (ex: CExpr) => ex match {

      case CDeDup(e1, l) => CDeDup(fapply(e1, funct), l)
      case CGroupBy(e1, v1, keys, values) => CGroupBy(fapply(e1, funct), v1, keys, values)
      case CReduceBy(e1, v1, keys, values) => CReduceBy(fapply(e1, funct), v1, keys, values)

      case CNamed(n, e1) => CNamed(n, fapply(e1, funct))
      case LinearCSet(es) => LinearCSet(es.map(e1 => fapply(e1, funct)))

      case AddIndex(in, v) => AddIndex(fapply(in, funct), v)
      case Projection(in, v, filter, fields, l) =>
        Projection(fapply(in, funct), v, filter, fields, l)
      case Nest(in, v, key, value, filter, nulls, ctag, l) => 
        Nest(fapply(in, funct), v, key, value, filter, nulls, ctag, l)
      case Unnest(in, v, path, v2, filter, fields, l) =>
        Unnest(fapply(in, funct), v, path, v2, filter, fields, l)
      case Join(left, v, right, v2, cond, fields, l) =>
        Join(fapply(left, funct), v, fapply(right, funct), v2, cond, fields, l)
      case OuterJoin(left, v, right, v2, cond, fields, l) =>
        OuterJoin(fapply(left, funct), v, fapply(right, funct), v2, cond, fields, l)
      case Reduce(e1, v1, key, value, l) => 
	  	  Reduce(fapply(e1, funct), v1, key, value, l)

      case GroupDict(e1) => GroupDict(fapply(e1, funct))
      case FlatDict(e1) => FlatDict(fapply(e1, funct))

      case _ => ex
    })
}
