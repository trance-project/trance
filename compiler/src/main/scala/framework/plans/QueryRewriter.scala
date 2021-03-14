package framework.plans

object QueryRewriter {

  // init rewrites give a ce
  // recall that a ce has:
  //   OR'd the filters
  //   UNION'd the projections and aggregations
  def rewritePlans(ce: CE): List[CExpr] = {
    List.empty[CExpr]
  }

  def rewritePlan(plan: CExpr, cover: CExpr): CExpr = plan match {
    //case r:Reduce =>  
    //case p:Projection => 
    //case s:Select => 
    case _ => plan
  
  } 

}
