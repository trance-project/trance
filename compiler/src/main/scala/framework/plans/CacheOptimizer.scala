package framework.plans

import framework.common._
import scala.collection.mutable.{Map, HashMap}

case class SE(subplan: CExpr, height: Int) {
  override def equals(that: Any): Boolean = that match {
    case that:SE => this.subplan == that.subplan
    case that:CExpr => this.subplan == that
    case _ => false
  }

  override def hashCode: Int = (subplan, height).hashCode
}

/** Optimizer used for plans from BatchUnnester **/
// class CacheOptimizer(schema: Schema = Schema()) extends Extensions {
object CacheOptimizer extends Extensions {

  def signature(plan: CExpr): Integer = {
    equivSig(plan)(HashMap.empty[CExpr, Integer])
  }

  def equivSig(plan: CExpr)(implicit sigmap: HashMap[CExpr, Integer]): Integer = {

    val sig = plan match {

      case j:JoinOp => 
        var lhash = equivSig(j.left)(sigmap)
        var rhash = equivSig(j.right)(sigmap)
        var cond = j.p1+"="+j.p2
        
        // agnostic to join order
        if (lhash > rhash) {
          val saveLeft = lhash
          lhash = rhash
          rhash = saveLeft
          cond = j.p2+"="+j.p1
        }
        hash(plan.getClass.toString + j.jtype + cond + lhash + rhash)

      case n:Nest => 
        hash(plan.getClass.toString + n.hashCode().toString)
      
      case u:UnnestOp => 
        val chash = equivSig(u.in)(sigmap)
        hash(plan.getClass.toString + u.outer + u.path + chash)
      
      case o:UnaryOp =>
        val chash = equivSig(o.in)(sigmap)
        hash(plan.getClass.toString + chash)

      case InputRef(n, t) => 
        hash(plan.getClass.toString + n)

      // all other cases
      case _ => hash(plan.getClass.toString + plan.hashCode().toString)

    }

    sigmap(plan) = sig
    sig

  }

  def subexpressions(plan: CExpr): HashMap[CExpr, Integer] = {
    val subexprs = HashMap.empty[CExpr, Integer]
    equivSig(plan)(subexprs)
    subexprs
  }

  def sharedSubs(plans: Vector[CExpr]): Map[Integer, List[SE]] = {
    
    val subexprs = plans.map(subexpressions(_))
    val sigmap = Map.empty[Integer, List[SE]].withDefaultValue(Nil)

    def traversePlan(plan: CExpr, index: Int, acc: Int = 0): Unit = plan match {
      // cache unfriendly
      case n:Nest => traversePlan(n.in, index, acc+1)
      case j:JoinOp => 
        val height = acc + 1
        traversePlan(j.left, index, height); traversePlan(j.right, index, height)
      
      // cache friendly
      case i:InputRef => 
        val sig = subexprs(index)(i)
        sigmap(sig) = sigmap(sig) :+ SE(i, acc)

      case o:UnaryOp =>
        val sig = subexprs(index)(o)
        sigmap(sig) = sigmap(sig) :+ SE(o, acc)
        traversePlan(o.in, index, acc+1)

      case _ => 

    }

    plans.zipWithIndex.map{ case (p, i) => traversePlan(p, i)}

    // values must have at least 
    // two elements to be shared
    sigmap.filter(_._2.size > 1)

  }

  private def hash(label: String): Integer = util.hashing.MurmurHash3.stringHash(label)

}