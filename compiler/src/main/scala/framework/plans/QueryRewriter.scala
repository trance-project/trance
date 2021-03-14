package framework.plans

object QueryRewriter extends Extensions {

  // init rewrites give a ce
  // recall that a ce has:
  //   OR'd the filters
  //   UNION'd the projections and aggregations
  def rewritePlans(ce: CE): CE = {
    CE(ce.cover, ce.sig, ce.ses.map(s => 
      SE(s.wid, rewritePlan(s.subplan, ce.cover), s.height)))
  }

  def rewritePlan(plan: CExpr, cover: CExpr): CExpr = plan match {
    //case r:Reduce =>  
    //case p:Projection => 
    //case s:Select => 
    case s:Select => 
      val cin = rewritePlan(s.in, cover)
      val v = Variable.freshFromBag(cin.tp)
      Select(cin, v, replace(s.p,v), v)
    case i:InputRef => cover
    case _ => plan
  
  } 

}
