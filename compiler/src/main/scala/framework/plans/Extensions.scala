package framework.plans

import framework.common._

/** Extensions to the plan language **/
trait Extensions {

  def containsCacheUnfriendly(plan: CExpr): Boolean = plan match {
    case o:UnaryOp => containsCacheUnfriendly(o.in)
    case _ => plan.isCacheUnfriendly
  }

  def find(e1: CExpr, e2: CExpr): Boolean = e1 match {
      case _ if (e1.vstr == e2.vstr) => true
      case c:CNamed => find(c.e, e2)
      case u:UnaryOp => find(u.in, e2)
      case j:JoinOp => find(j.left, e2) || find(j.right, e2)
      case n:Nest => find(n.in, e2)
      case _ => false

    }

  // Given an expression e replaces all variable occurrences in e with v. 
  // example: (a := x1.a, b := x2.b) => (a := v.a, b := v.b)
  // 
  // optional type check (useType)
  def replace(e: CExpr, v: Variable, useType: Boolean = false): CExpr = e match {
    case Record(ms) => Record(ms.map(r => r._1 -> replace(r._2, v, useType)))
    case Label(ms) => Label(ms.map(r => r._1 -> replace(r._2, v, useType)))
    case Project(_, f) => Project(v, f)
    case If(cond, s1, Some(s2)) => If(replace(cond, v, useType), replace(s1, v, useType), Some(replace(s2, v, useType)))
    case If(cond, s1, None) => If(replace(cond, v, useType), replace(s1, v, useType), None)
    case Equals(e1, e2) => Equals(replace(e1, v, useType), replace(e2, v, useType))
    case And(e1, e2) => And(replace(e1, v, useType), replace(e2, v, useType))
    case MathOp(op, e1, e2) => MathOp(op, replace(e1, v, useType), replace(e2, v, useType))
    // this will be additional base ops
    case v1:Variable if useType && v1.tp == v.tp => v
    case v1:Variable if !useType => v 
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

  def collectFromAnd(e: CExpr): Set[String] = e match {
    case Record(e1) => e1.flatMap(f => collect(f._2)).toSet
    case Label(e1) => e1.flatMap(f => collect(f._2)).toSet
    case If(cond, s1, Some(s2)) => collect(cond) ++ collect(s1) ++ collect(s2)
    case If(cond, s1, None) => collect(cond) ++ collect(s1)
    case MathOp(op, e1, e2) => collect(e1) ++ collect(e2)
    case Equals(e1, e2) => collect(e1) ++ collect(e2)
    case And(e1, e2) => collect(e1) ++ collect(e2)
    case Project(e1, f) => Set(f)
    case _ => Set()
  }

  def fapply(e: CExpr, funct: PartialFunction[CExpr, CExpr]): CExpr = 
    funct.applyOrElse(e, (ex: CExpr) => ex match {

      case CDeDup(e1, l) => CDeDup(fapply(e1, funct), l)
      case CGroupBy(e1, v1, keys, values, gname) => CGroupBy(fapply(e1, funct), v1, keys, values, gname)

      case CReduceBy(e1, v1, keys, values) => CReduceBy(fapply(e1, funct), v1, keys, values)

      case CNamed(n, e1) => CNamed(n, fapply(e1, funct))
      case LinearCSet(es) => LinearCSet(es.map(e1 => fapply(e1, funct)))

      case AddIndex(in, v) => AddIndex(fapply(in, funct), v)

      case RemoveNulls(in) => RemoveNulls(fapply(in, funct))
        
      case Projection(in, v, filter, fields, l) =>
        Projection(fapply(in, funct), v, filter, fields, l)
      
      case Nest(in, v, key, value, filter, nulls, ctag, l) => 
        Nest(fapply(in, funct), v, key, value, filter, nulls, ctag, l)
      
      case OuterUnnest(in, v, path, v2, filter, fields, l) =>
        OuterUnnest(fapply(in, funct), v, path, v2, filter, fields, l)
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
