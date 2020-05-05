package framework.plans

import framework.core._

/** Extensions for the plan language **/
trait Extensions {

  def substitute(e: CExpr, vold: Variable, vnew: Variable): CExpr = e match {
    case Record(fs) => Record(fs.map{ 
      case (attr, e2) => attr -> substitute(e2, vold, vnew)})
    case Project(v, f) => 
      Project(substitute(v, vold, vnew), f)
    case v @ Variable(_,_) => 
      if (v == vold) Variable(vnew.name, v.tp) else v
    case _ => e
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
      case _ => ex
    })
}
