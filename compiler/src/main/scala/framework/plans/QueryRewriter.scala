package framework.plans

import framework.common._
import scala.collection.immutable.{Map => IMap}
import scala.collection.mutable.{Map, HashMap}

// move the covers here as well..
class QueryRewriter(sigs: HashMap[(CExpr, Int), Integer] = HashMap.empty[(CExpr, Int), Integer]) extends Extensions {

  // init rewrites give a ce
  // recall that a ce has:
  //   OR'd the filters
  //   UNION'd the projections and aggregations
  def rewritePlans(ce: CE): CE = {
    CE(ce.cover, ce.sig, ce.ses.map(s => 
      SE(s.wid, rewritePlan(s.subplan, ce.name, ce.cover), s.height)))
  }

  def rewritePlans(plans: Vector[(CExpr, Int)], covers: IMap[Integer, CNamed]): Vector[CExpr] = {
    plans.map(p => rewritePlanOverCover(p, covers))
  }


  // for each plan 
  //   if plan is an immedate similar expression of cover in cache
  //      then rewrite plan over cover 
  //   else rewrite plan with recurse on subplan
  //
  // a join and a nest will never be a subexpression, so this makes sure that 
  // anything beneath it gets replaced with the proper subexpression
  // 
  // this also handles the case where multiple subexpressions are in the cache
  def rewritePlanOverCover(plan: (CExpr, Int), covers: IMap[Integer, CNamed]): CExpr = {

    val sig = sigs(plan)
    covers.get(sig) match {

      // in subexpression list
      case Some(cover) => 
        rewritePlan(plan._1, cover.name, cover.e)

      // not in cover, see if subexpression is
      case None => plan match {

        case (j:JoinOp, id) => 
          assert(j.isEquiJoin)
          val lCover = rewritePlanOverCover((j.left, id), covers)
          val rCover = rewritePlanOverCover((j.right, id), covers)
          val lv = Variable.freshFromBag(lCover.tp)
          val rv = Variable.freshFromBag(rCover.tp)

          val cond = Equals(Project(lv, j.p1), Project(rv, j.p2))

          if (j.jtype.contains("outer")) OuterJoin(lCover, lv, rCover, rv, cond, j.fields)
          else Join(lCover, lv, rCover, rv, cond, j.fields)

        case (n: Nest, id) => 
          val childCover = rewritePlanOverCover((n.in, id), covers)
          val v = Variable.freshFromBag(childCover.tp)
          Nest(childCover, v, n.key, replace(n.value, v), replace(n.filter, v), n.nulls, n.ctag)

        case (r:Reduce, id) => 
          val childCover = rewritePlanOverCover((r.in, id), covers)
          val v = Variable.freshFromBag(childCover.tp)
          Reduce(childCover, v, r.keys, r.values)

        case (u:UnnestOp, id) => 
          val childCover = rewritePlanOverCover((u.in, id), covers)
          val v = Variable.freshFromBag(childCover.tp)
          if (u.outer) OuterUnnest(childCover, v, u.path, u.v2, u.filter, u.fields)
          else Unnest(childCover, v, u.path, u.v2, u.filter, u.fields)

        case (p:Projection, id) => 
          val childCover = rewritePlanOverCover((p.in, id), covers)
          val v = Variable.freshFromBag(childCover.tp)
          Projection(childCover, v, replace(p.filter, v), p.fields)

        case (s:Select, id) => 
          val childCover = rewritePlanOverCover((s.in, id), covers)
          val v = Variable.freshFromBag(childCover.tp)
          Select(childCover, v, replace(s.p, v), v)

        case (i:AddIndex, id) => 
          // is subexpression a cover?
          val childCover = rewritePlanOverCover((i.in, id), covers)
          AddIndex(childCover, i.name)

        case _ => plan._1
      }

    }

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

      case (u1:UnnestOp, u2:UnnestOp) => 
        assert(u1.path == u2.path)
        Projection(cover, v, replace(u1.filter, v), u1.fields)

      // reapply the projection
      case (p1:Projection, p2:Projection) => 
        // handle outer to inner join
        val fields = (p1.in, p2.in) match {
          case (_:Join, _:OuterJoin) => p1.filter.tp.attrs.keySet.toList :+ "remove_nulls"
          case _ => p1.filter.tp.attrs.keySet.toList
        }
        Projection(cover, v, p1.filter, fields)

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

object QueryRewriter {
  def apply() = new QueryRewriter(sigs = HashMap.empty[(CExpr, Int), Integer])
  def apply(sigs: HashMap[(CExpr, Int), Integer]) = new QueryRewriter(sigs = sigs)
}
