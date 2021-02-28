package framework.plans

import framework.common._
import scala.collection.mutable.{Map, HashMap}

case class SE(wid: Int, name: String, subplan: CExpr, height: Int) {
  override def equals(that: Any): Boolean = that match {
    case that:SE => this.subplan == that.subplan
    case that:CExpr => this.subplan == that
    case _ => false
  }

  override def hashCode: Int = (subplan, height).hashCode
}

object SE {
  // empty name
  def apply(wid: Int, subplan: CExpr, height: Int): SE = SE(wid, "", subplan, height)
  def apply(wid: Int, name: String, subplan: CExpr, height: Int): SE = SE(wid, name, subplan, height)
}

/** Optimizer used for plans from BatchUnnester **/
object SEBuilder extends Extensions {

  def signature(plan: CExpr): Integer = {
    equivSig((plan, 0))(HashMap.empty[(CExpr, Int), Integer])
  }

  def equivSig(plan: (CExpr, Int))(implicit sigmap: HashMap[(CExpr, Int), Integer]): Integer = {

    val wid = plan._2
    val sig = plan._1 match {

      case j:JoinOp => 
        var lhash = equivSig((j.left, wid))(sigmap)
        var rhash = equivSig((j.right, wid))(sigmap)
        var cond = j.p1+"="+j.p2
        
        // agnostic to join order
        if (lhash > rhash) {
          val saveLeft = lhash
          lhash = rhash
          rhash = saveLeft
          cond = j.p2+"="+j.p1
        }
        hash(plan.getClass.toString + j.jtype + cond + lhash + rhash)

      // case n:Nest => 
      //   hash(plan.getClass.toString + n.hashCode().toString)
      
      case u:UnnestOp => 
        val chash = equivSig((u.in, wid))(sigmap)
        hash(plan.getClass.toString + u.outer + u.path + chash)
      
      case o:UnaryOp =>
        val chash = equivSig((o.in, wid))(sigmap)
        hash(plan.getClass.toString + chash)

      case InputRef(n, t) => 
        hash(plan.getClass.toString + n)

      case CNamed(name, p) => equivSig((p, wid))
 
      // all other cases
      case _ => hash(plan.getClass.toString + plan.hashCode().toString)

    }

    sigmap(plan) = sig
    sig

  }

  def subexpressions(plan: CExpr): HashMap[(CExpr, Int), Integer] = {
    subexpressions((plan, 0))
  }

  def subexpressions(plan: (CExpr, Int)): HashMap[(CExpr, Int), Integer] = {
    val subexprs = HashMap.empty[(CExpr, Int), Integer]
    equivSig(plan)(subexprs)
    subexprs
  }

  // pass in programs
  def sharedSubsFromProgram(plans: Vector[LinearCSet]): Map[Integer, List[SE]] = {
    val seInput = plans.zipWithIndex.flatMap{ case (prog, id) => 
      prog.exprs.map(e => (e, id))
    }
    println(seInput.size)
    sharedSubs(seInput)
  }

  def sharedSubs(plans: Vector[(CExpr, Int)]): Map[Integer, List[SE]] = {
    
    val subexprs = plans.map(subexpressions(_))
    val sigmap = Map.empty[Integer, List[SE]].withDefaultValue(Nil)

    def traversePlan(plan: (CExpr, Int), index: Int, acc: Int = 0, name: String = ""): Unit = plan match {
      // cache unfriendly
      case (n:Nest, id) => traversePlan((n.in, id), index, acc+1, name)
      case (j:JoinOp, id) => 
        val height = acc + 1
        traversePlan((j.left, id), index, height, name); traversePlan((j.right, id), index, height, name)
      
      // cache friendly
      case (i:InputRef, id) => 
        val sig = subexprs(index)(plan)
        sigmap(sig) = sigmap(sig) :+ SE(id, name, i, acc)

      // should unnest be considered cache unfriendly?
      case (o:UnaryOp, id) =>
        val sig = subexprs(index)(plan)
        sigmap(sig) = sigmap(sig) :+ SE(id, name, o, acc)
        traversePlan((o.in, id), index, acc+1, name)

      // is this correct?
      // case (c @ CNamed(name, p), id) => 
      //   traversePlan((p, id), index, acc, name)

      case _ => 

    }

    plans.zipWithIndex.map{ case (p, i) => traversePlan(p, i)}

    // values must have at least 
    // two elements to be shared
    sigmap.filter(_._2.size > 1)

  }

  private def hash(label: String): Integer = util.hashing.MurmurHash3.stringHash(label)

}