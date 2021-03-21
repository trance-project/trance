package framework.plans

import framework.common._
import scala.collection.mutable.{Map, HashMap}
import java.util.UUID.randomUUID

case class CE(name: String, cover: CExpr, sig: Integer, ses: List[SE])

object CE{
  def apply(cover: CExpr, sig: Integer, ses: List[SE]): CE = 
    CE("", cover, sig, ses)
}

object CEBuilder extends Extensions {

  val normalizer = new BaseNormalizer{}
  import normalizer._

  val vmap = Map.empty[String, String]

  // todo preserve height
  def buildCoverFromSE(plans: List[SE]): CExpr = {
    val plan1 = plans.head.subplan
    val ce1 = plans.tail.map(se => se.subplan).reduce((ce, p) => buildCover(ce, p))
    buildCover(plan1, ce1)
  }

  // renames, assumes it is in the cache - used for testing
  def buildCovers(subs: Map[Integer, List[SE]]): List[CE] = subs.map{
    case (id, se) => 
      val cover = buildCoverFromSE(se)
      val cnamed = "Cover"+randomUUID().toString().replace("-", "")
      CE(cnamed, cover, id, se)
  }.toList

  def buildCoverMap(subs: Map[Integer, List[SE]]): scala.collection.immutable.Map[Integer, CNamed] = subs.flatMap{
    case (sig, ses) =>  
      if (!ses.head.subplan.isCacheUnfriendly){
        Seq((sig, CNamed("Cover"+randomUUID().toString().replace("-", ""), buildCover(ses.map(_.subplan)))))
      }else Nil
  }.toMap

  def buildCover(plans: List[CExpr]): CExpr = plans match {
    case head :: Nil => head
    case head :: tail => 
      val ce1 = tail.reduce((ce, p) => buildCover(ce, p))
      buildCover(head, ce1)
    case _ => sys.error("empty subepxression list")
  }

  // cover should not rename anything
  def buildCover(plan1: CExpr, plan2: CExpr): CExpr = (plan1, plan2) match {
    
    case y if plan1.vstr == plan2.vstr => plan1

    case (Reduce(in1, v1, ks1, vs1), Reduce(in2, v2, ks2, vs2)) => 
      val child = buildCover(in1, in2)
      val ks = ks1.toSet ++ ks2.toSet
      val vs = vs1.toSet ++ vs2.toSet
      val v = Variable.freshFromBag(child.tp)
      Reduce(child, v, ks.toList.map(k => vmap.getOrElse(k, k)), 
        vs.toList.map(v => vmap.getOrElse(v, v)))

    case (u1:UnnestOp, u2:UnnestOp) => 
      assert(u1.path == u2.path)
      val child = buildCover(u1.in, u2.in)
      val v = Variable.freshFromBag(child.tp)
      val v2 = Variable.freshFromBag(v.tp.asInstanceOf[RecordCType](u1.path))
      // assume lower level, since it should be pushed
      val cond = or(replace(u1.filter, v2), replace(u2.filter, v2))
      if (u1.outer) OuterUnnest(child, v, u1.path, v2, cond, u1.fields ++ u2.fields)
      else Unnest(child, v, u1.path, v2, cond, u1.fields ++ u2.fields)

    // capture below joins
    case (j1:JoinOp, j2:JoinOp) =>
      assert(Set(j1.p1, j1.p2) == Set(j2.p1, j2.p2))

      val (left, right) = 
        if (SEBuilder.signature(j1.left) == SEBuilder.signature(j2.left))
          (buildCover(j1.left, j2.left), buildCover(j1.right, j2.right))
        else (buildCover(j1.left, j2.right), buildCover(j1.right, j2.left))

      val v1 = Variable.freshFromBag(left.tp)
      val v2 = Variable.freshFromBag(right.tp)
      val cond = Equals(Project(v1, j1.p1), Project(v2, j1.p2))

      if (j1.jtype == "left_outer" || j2.jtype == "left_outer") OuterJoin(left, v1, right, v2, cond, j1.fields ++ j2.fields)
      else Join(left, v1, right, v2, cond, j1.fields ++ j2.fields)
    
    // union columns
    // TODO no implicit renaming should happen in the cover expression
    case (Projection(in1, v1, f1:Record, fs1), Projection(in2, v2, f2:Record, fs2)) => 
    
      val child = buildCover(in1, in2)
      val v = Variable.freshFromBag(child.tp)
      def updateVmap(r: Record): scala.collection.immutable.Map[String, CExpr] = {
        r.fields.map{
          case (field1, Project(_, field2)) =>
            vmap(field1) = field2
            (field2, Project(v, field2))
          case _ => ???
        }.toMap
      }
      val r = Record(updateVmap(f1) ++ updateVmap(f2))
      val nr = replace(r, v)
      Projection(child, v, nr, fs1 ++ fs2)

   // OR filters
    case (Select(in1, v1, f1, e1), Select(in2, v2, f2, e2)) =>
      assert(in1.tp == in2.tp)
      val v = Variable.fresh(in1.tp)
      val child = buildCover(in1, in2)
      Select(child, v, or(replace(f1, v), replace(f2, v)), v)

    case _ =>  sys.error(s"unsupported operator $plan1, $plan2)")

  }

}
