package framework.plans

import framework.common._

object QueryRewriter extends Extensions {

  // init rewrites give a ce
  // recall that a ce has:
  //   OR'd the filters
  //   UNION'd the projections and aggregations
  def rewritePlans(ce: CE): CE = {
    CE(ce.cover, ce.sig, ce.ses.map(s => 
      SE(s.wid, rewritePlan(s.subplan, ce.name, ce.cover), s.height)))
  }

  def rewritePlan(plan: CExpr, name:String, coverPlan: CExpr): CExpr = {
  
    val cover = Variable(name, coverPlan.tp)
    val v = Variable.freshFromBag(cover.tp)

    (plan, coverPlan) match {

      case (r1 @ Reduce(p @ Projection(in, _, r:Record, fs), _, ks1, vs1), r2:Reduce) =>
        
        val names = r.fields.map(f => f match {
            case (field1, Project(_, field2)) => (field1, field2)
            case _ => ???
          })

        if (ks1.map(k => names.getOrElse(k, k)).toSet == r2.keys.toSet) {
          Projection(cover, v, replace(r, v), fs)
        } else {
          val p1 = Projection(cover, v, replace(r, v), fs)
          val v1 = Variable.freshFromBag(p1.tp)
          Reduce(p1, v1, ks1, vs1)
        }  

      case (u1:Unnest, u2:Unnest) => 
        assert(u1.path == u2.path)
        Projection(cover, v, replace(u1.filter, v), u1.fields)

      // reapply the projection
      case (p:Projection, _) => 
        Projection(cover, v, p.filter, p.filter.tp.attrs.keySet.toList)

      // reapply the filter
      case (s:Select, _) => 
        Select(cover, v, replace(s.p, v), v)

      // the index may or may not be needed...
      case (i:AddIndex, _) => AddIndex(cover, i.name)

      // input refs just are replaced with the cover
      case (i:InputRef, _) => cover

      // all other cases don't rewrite
      case _ => plan
    
    } 

  }

}
