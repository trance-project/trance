package framework.plans

import framework.common._
import scala.collection.mutable.{Map, HashMap}

case class SE(wid: Int, subplan: CExpr, height: Int) {
  override def equals(that: Any): Boolean = that match {
    case that:SE => this.subplan == that.subplan
    case that:CExpr => this.subplan == that
    case _ => false
  }

  override def hashCode: Int = (subplan, height).hashCode
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

      case n:Nest => 
        val chash = equivSig((n.in, wid))(sigmap)
        hash(plan.getClass.toString + n.hashCode().toString + chash)
      
      case u:UnnestOp => 
        val chash = equivSig((u.in, wid))(sigmap)
        hash(plan.getClass.toString + u.outer + u.path + chash)
      
      case o:UnaryOp =>
        val chash = equivSig((o.in, wid))(sigmap)
        hash(plan.getClass.toString + chash)

      case InputRef(n, t) => 
        hash(plan.getClass.toString + n)
 
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
    // for now by-pass cnamed
    val seInput = plans.zipWithIndex.flatMap{ case (prog, id) => 
      prog.exprs.map(e => e match {
        case CNamed(name, p) => (p, id)
        case _ => (e, id)
    })}
    val subexprs = HashMap.empty[(CExpr, Int), Integer]
    seInput.foreach(p => subexpressions(p))
    sharedSubs(seInput, subexprs)
  }

  def containsCacheUnfriendly(plan: CExpr): Boolean = plan match {
    case o:UnaryOp => containsCacheUnfriendly(o.in)
    case (_:Nest) | (_:JoinOp) => true
    case _ => false
  }

  def sharedSubs(plans: Vector[(CExpr, Int)], subexprs: HashMap[(CExpr, Int), Integer], 
    limit: Boolean = false): Map[Integer, List[SE]] = {
    
    val sigmap = Map.empty[Integer, List[SE]].withDefaultValue(Nil)

    def traversePlan(plan: (CExpr, Int), index: Int, acc: Int = 0): Unit = plan match {

      // the issue here is that the nest and the join should be 
      // rewritten in order to use the cover expression
      // if there is not a limit, we should probably put these in the 
      // similar expression so that they will be rewritten IF the similar 
      // expression is used (how does verona handle this case...)
      case (n:Nest, id) => 
        if (!limit){
          val sig = subexprs(plan)
          sigmap(sig) = sigmap(sig) :+ SE(id, n, acc)
        }

        traversePlan((n.in, id), index, acc+1)
      
      // should be added to use the covering expression
      case (j:JoinOp, id) => 

        if (!limit){
          val sig = subexprs(plan)
          sigmap(sig) = sigmap(sig) :+ SE(id, j, acc)
        }

        val height = acc + 1
        traversePlan((j.left, id), index, height) 
        traversePlan((j.right, id), index, height)
      
      case (i:InputRef, id) => 
        val sig = subexprs(plan)
        sigmap(sig) = sigmap(sig) :+ SE(id, i, acc)

      case (o:UnaryOp, id) =>
        println("did we get here?")
        println(Printer.quote(o))
        val sig = subexprs(plan)
        sigmap(sig) = sigmap(sig) :+ SE(id, o, acc)

        if (!limit) traversePlan((o.in, id), index, acc+1)

      case _ =>    

    }

    plans.zipWithIndex.map{ case (p, i) => traversePlan(p, i)}

    // values must have at least 
    // two elements to be shared
    sigmap.filter(_._2.size > 1)
    // sigmap.filter{ ses => 
    //   if (ses._2.nonEmpty) {
    //     ses._2.head match {
    //       case _:Nest | _:JoinOp => false
    //       case _ => true
    //     }  
    //   } else false 
    // }

  }

  private def hash(label: String): Integer = util.hashing.MurmurHash3.stringHash(label)

}