package framework.plans

import framework.common._
// import scala.collection.mutable.{Map, HashMap}

object CEBuilder extends Extensions {

  val normalizer = new BaseNormalizer{}
  import normalizer._

  def buildCover(plans: List[CExpr]): CExpr = {
    val plan1 = plans.head
    val ce1 = plans.tail.reduce((ce, p) => buildCover(ce, p))
    buildCover(plan1, ce1)
  }

  def buildCover(plan1: CExpr, plan2: CExpr): CExpr = (plan1, plan2) match {
    
    case y if plan1 == plan2 => plan1
    
    case (Projection(in1, v1, f1:Record, fs1), Projection(in2, v2, f2:Record, fs2)) => 
      // assert(in1.tp == in2.tp)
      val r = Record(f1.fields ++ f2.fields)
      val v = Variable.fresh(r.tp)
      Projection(in1, v, replace(r, v), fs1 ++ fs2)

    case (Select(in1, v1, f1, e1), Select(in2, v2, f2, e2)) =>
      assert(in1.tp == in2.tp)
      val v = Variable.fresh(in1.tp)
      Select(in1, v, or(replace(f1, v), replace(f2, v)), v)

    case _ =>  sys.error(s"unsupported operator )$plan1, $plan2)")

  }

}