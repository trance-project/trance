package framework.plans

import framework.common._
import framework.utils._

object NRCUnnester extends PlanOperator {

  type Ctx = (Option[VarDef], Option[VarDef], Option[PExpr])
  @inline def u(implicit ctx: Ctx): Option[VarDef] = ctx._1 
  @inline def w(implicit ctx: Ctx): Option[VarDef] = ctx._2 
  @inline def E(implicit ctx: Ctx): Option[PExpr] = ctx._3

  val truef = PrimitiveConst(true, BoolType)
  val es: Set[String] = Set.empty[String]

  def isNested(e: Expr): Boolean = e match {
    case _:ForeachUnion => true
    case _ => false
  }

  def unnest(e: Expr)(implicit ctx: Ctx): PExpr = e match {

    case ForeachUnion(x, e1, e2) if E.isEmpty => 
      val nE = Select(e1, x, truef)
      unnest(e2)((u, Some(x), Some(nE)))

    case ForeachUnion(x, e1, BagIfThenElse(cond, e2, None)) => 
      val ne1 = unnest(e1)((u, w, None))
      val nE = OuterJoin(E.get, w.get, ne1, x, cond, es)
      unnest(e2)((u, Some(w.get.batch(nE.tp)), Some(nE)))

    case ForeachUnion(x, e1, e2) => 
      val ne1 = unnest(e1)((u, w, None))
      val nE = OuterJoin(E.get, w.get, ne1, x, truef, es)
      unnest(e2)((u, Some(w.get.batch(nE.tp)), Some(nE)))

    case Singleton(t @ Tuple(fs)) =>

      fs.find(c => isNested(c._2)) match {

        case Some((key, value)) => 

          val nE = unnest(value)((w, w, E))
          // tag with "_2 default"
          val nV = w.get.batch(nE.tp)
          val bv = BagProject(TupleVarRef(nV.name, nV.tp.asInstanceOf[TupleType]), "_2")

          val ne = Singleton(Tuple((fs + (key -> bv)).asInstanceOf[Map[String, TupleAttributeExpr]]))
          unnest(ne)((u, Some(w.get.batch(ne.tp)), Some(nE)))

        case y if u.isEmpty => Projection(E.get, w.get, t)

        case _ => Nest(E.get, w.get, u.get.keys, t, truef, (w.get.keys -- u.get.keys)) 

      } 

    case BagVarRef(n, tp) => 
      val v = VarDef(Utils.Symbol.fresh(), tp.tp)
      Select(e, v, truef)

    case _ => sys.error(s"unsupported expression $e")

  }

  def unnest(e: Assignment): PExpr = Plan(e.name, unnest(e.rhs)((None, None, None)))

  def unnest(e: Program): PExpr = PlanSet(e.statements.map(a => unnest(a)))



}