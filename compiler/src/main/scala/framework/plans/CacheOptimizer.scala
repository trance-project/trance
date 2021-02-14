package framework.plans

import framework.common._

/** Optimizer used for plans from BatchUnnester **/
// class CacheOptimizer(schema: Schema = Schema()) extends Extensions {
object CacheOptimizer extends Extensions {

  def equiv(plan: CExpr): Integer = plan match {

    case Reduce(in, _, _, _) => 
      val chash = equiv(in)
      hash(plan.getClass.toString + chash)

    case u:UnnestOp => 
      val chash = equiv(u.in)
      hash(plan.getClass.toString + u.outer + u.path + chash)

    case j:JoinOp => 
      var lhash = equiv(j.left)
      var rhash = equiv(j.right)
      var cond = j.p1+"="+j.p2
      
      // agnostic to join order
      if (lhash > rhash) {
        val saveLeft = lhash
        lhash = rhash
        rhash = saveLeft
        cond = j.p2+"="+j.p1
      }
      hash(plan.getClass.toString + j.jtype + cond + lhash + rhash)

    // unary
    case Projection(in, _, _, _) => 
      val chash = equiv(in)
      hash(plan.getClass.toString + chash)

    case Select(x, _, _, _) => 
      val chash = equiv(x)
      hash(plan.getClass.toString + chash)

    // leaf
    case InputRef(n, t) => 
      hash(plan.getClass.toString + n)

    // this will capture nest cases
    case _ => hash(plan.getClass.toString + plan.hashCode().toString)

  }

  private def hash(label: String): Integer = util.hashing.MurmurHash3.stringHash(label)

}