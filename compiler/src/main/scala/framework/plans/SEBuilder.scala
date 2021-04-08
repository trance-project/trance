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
class SEBuilder(progs: Vector[(CExpr, Int)]) extends Extensions {

  val subexprs: HashMap[(CExpr, Int), Integer] = HashMap.empty[(CExpr, Int), Integer]

  val tmpMap: Map[String, Integer] = Map.empty[String, Integer]

  def getSubexprs(): HashMap[(CExpr, Int), Integer] = subexprs
  def getNameMap: Map[String, Integer] = Map.empty[String, Integer]

  def updateSubexprs(): Unit = progs.foreach(p => equivSig(p)(subexprs))

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
        hash(j.getClass.toString + cond + lhash + rhash)

      case n:Nest => 
        val chash = equivSig((n.in, wid))(sigmap)
        hash(n.getClass.toString + n.hashCode().toString + chash)

      case u:UnnestOp => 
        val chash = equivSig((u.in, wid))(sigmap)
        hash(u.getClass.toString + u.path + chash)

      case f:FlatDict => 
        equivSig((f.in, wid))(sigmap)

      case g:GroupDict => 
        equivSig((g.in, wid))(sigmap)

      case r:Reduce =>
        val chash = equivSig((r.in, wid))(sigmap)
        hash(r.getClass.toString() + chash)

      case o:UnaryOp =>
        val chash = equivSig((o.in, wid))(sigmap)
        hash(o.getClass.toString() + chash)

      case i:InputRef => 
        tmpMap.getOrElse(i.data, hash(i.getClass.toString + i.data))

      case c:CNamed => 
        val chash = equivSig((c.e, wid))(sigmap)
        tmpMap(c.name) = chash
        hash(c.getClass.toString + c.name + chash)

      case l:LinearCSet => 
        val chash = hash(l.exprs.map(e => (equivSig((e, wid))(sigmap)).toString).reduce(_ + _))
        hash(l.getClass.toString + chash)

      // all other cases
      case _ => hash(plan._2.getClass.toString + plan._2.hashCode().toString)

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

  def sharedSubs(limit: Boolean = true): Map[Integer, List[SE]] = {
    
    val sigmap = Map.empty[Integer, List[SE]].withDefaultValue(Nil)

    def traversePlan(plan: (CExpr, Int), acc: Int = 0): Unit = plan match {

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

        traversePlan((n.in, id), acc+1)
      
      // should be added to use the covering expression
      case (j:JoinOp, id) => 

        if (!limit){
          val sig = subexprs(plan)
          sigmap(sig) = sigmap(sig) :+ SE(id, j, acc)
        }

        val height = acc + 1
        traversePlan((j.left, id), height) 
        traversePlan((j.right, id), height)
      
      case (i:InputRef, id) => 
        val sig = hash("input" + subexprs(plan))
        sigmap(sig) = sigmap(sig) :+ SE(id, i, acc)

      case (f:FlatDict, id) => 
        traversePlan((f.in, id), acc)

      case (g:GroupDict, id) => 
        traversePlan((g.in, id), acc)

      // handle outer unnests
      case (o:UnaryOp, id) => 
        val sig = subexprs(plan)
        sigmap(sig) = sigmap(sig) :+ SE(id, o, acc)

        traversePlan((o.in, id), acc+1)

      // cname should never be a cover
      // a domain should never be a cache candidate?
      case (c:CNamed, id) =>
        if (!c.name.contains("Dom")){

          // val sig = subexprs(plan)
          // sigmap(sig) = sigmap(sig) :+ SE(id, c, acc)

          traversePlan((c.e, id), acc+1)
        }

      case (l:LinearCSet, id) => 
        l.exprs.foreach(p => traversePlan((p, id), acc+1))

      case _ =>  sys.error(s"unimplemented $plan")  

    }

    progs.map(p => traversePlan(p))

    // return all filter in covers
    // values must have at least 
    // two elements to be shared
    sigmap

  }

  private def hash(label: String): Integer = util.hashing.MurmurHash3.stringHash(label)

}

object SEBuilder {

  def apply(progs: Vector[(CExpr, Int)]): SEBuilder = new SEBuilder(progs)
  def apply(): SEBuilder = new SEBuilder(Vector.empty[(CExpr, Int)])

}