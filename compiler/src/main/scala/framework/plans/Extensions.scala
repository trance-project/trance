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
    case _ => Set()
  }

  def fapply(e: CExpr, funct: PartialFunction[CExpr, CExpr]): CExpr = 
    funct.applyOrElse(e, (ex: CExpr) => ex match {
      case Reduce(d, v, f, p) => Reduce(fapply(d, funct), v, f, p)
      case Nest(e1, v1, f, e, v, p, g, dk) => Nest(fapply(e1, funct), v1, f, e, v, p, g, dk)
      case Unnest(e1, v1, f, v2, p, value) => Unnest(fapply(e1, funct), v1, f, v2, p, value)
      case OuterUnnest(e1, v1, f, v2, p, value) => OuterUnnest(fapply(e1, funct), v1, f, v2, p, value)
      case Join(e1, e2, v1, p1, v2, p2, proj1, proj2) => Join(fapply(e1, funct), fapply(e2, funct), v1, p1, v2, p2, proj1, proj2)
      case OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2) => OuterJoin(fapply(e1, funct), fapply(e2, funct), v1, p1, v2, p2, proj1, proj2)
      case Lookup(e1, e2, v1, p1, v2, p2, p3) => Lookup(fapply(e1, funct), e2, v1, p1, v2, p2, p3)
      case CDeDup(e1) => CDeDup(fapply(e1, funct))
      case GroupDict(e1) => GroupDict(fapply(e1, funct))
      case FlatDict(e1) => FlatDict(fapply(e1, funct))
      case CGroupBy(e1, v1, keys, values) => CGroupBy(fapply(e1, funct), v1, keys, values)
      case CReduceBy(e1, v1, keys, values) => CReduceBy(fapply(e1, funct), v1, keys, values)
      case CNamed(n, e1) => CNamed(n, fapply(e1, funct))
      case LinearCSet(es) => LinearCSet(es.map(e1 => fapply(e1, funct)))
      // extend these
      case DFProject(in, v, filter, fields) =>
        DFProject(fapply(in, funct), v, filter, fields)
      case DFNest(in, v, key, value, filter, nulls, ctag) => DFNest(fapply(in, funct), v, key, value, filter, nulls, ctag)
      case DFUnnest(in, v, path, v2, filter, fields) =>
        DFUnnest(fapply(in, funct), v, path, v2, filter, fields)
      case DFJoin(left, v, right, v2, cond, fields) =>
        DFJoin(fapply(left, funct), v, fapply(right, funct), v2, cond, fields)
      case DFOuterJoin(left, v, right, v2, cond, fields) =>
        DFOuterJoin(fapply(left, funct), v, fapply(right, funct), v2, cond, fields)
      case AddIndex(in, v) => AddIndex(fapply(in, funct), v)
      case _ => ex
    })
}
//(in: CExpr, v: Variable, key: List[String], value: CExpr, filter: CExpr, nulls: List[String], ctag: String)
