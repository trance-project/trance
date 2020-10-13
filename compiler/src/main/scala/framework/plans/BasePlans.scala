package framework.plans

import framework.common._
import framework.utils._

trait BasePlan extends PlanOperator {

  type Source
  type Target

  def select(in: Source, p: Source => Source): Target
  def index(in: Source, name: String): Target
  def projection(in: Target, t: Source => Source): Target //fields: Set[String]): Target
  def outerjoin(lin: Target, rin: Target, cond: (Source, Source) => Source, fields: Set[String]): Target
  def nest(in: Target, key: Set[String], value: Source => Source, filter: Source => Source, nulls: Set[String]): Target

}

trait BasePlanCompiler extends BasePlan {
  
  type Source = Expr
  type Target = PExpr

  def select(in: Source, p: Source => Source): Target = {
    val v = fresh(in.tp)
    Select(in, v.varDef, p(v))
  }

  def index(in: Source, name: String): Target = 
    Index(in, name)

  def projection(in: Target, t: Source => Source): Target = {//fields: Set[String]): Target = {
    val v = fresh(in.tp)
    Projection(in, v.varDef, t(v))//fields)
  }

  def outerjoin(lin: Target, rin: Target, cond: (Source, Source) => Source, fields: Set[String]): Target = {
    val lv = fresh(lin.tp)
    val rv = fresh(rin.tp)
    OuterJoin(lin, lv.varDef, rin, rv.varDef, cond(lv, rv), fields)
  }

  def nest(in: Target, key: Set[String], value: Source => Source, filter: Source => Source, nulls: Set[String]): Target = {
    val v = fresh(in.tp)
    Nest(in, v.varDef, key, value(v), filter(v), nulls)
  }

  def fresh(tp: Type): TupleVarRef = tp match {
    case BagType(tup) => TupleVarRef(Utils.Symbol.fresh(), tup)
    case _ => sys.error(s"unsupported type $tp")
  }

}

class PlanFinalizer(val target: BasePlan, val source: BaseNRC) extends PlanOperator {

  var variableMap: Map[VarDef, target.Source] = Map[VarDef, target.Source]()
  
  def withMap[T](m: (VarDef, target.Source))(f: => T): T = {
    val old = variableMap
    variableMap = variableMap + m
    val res = f
    variableMap = old
    res
  }

  def withMapList[T](m: List[(VarDef, target.Source)])(f: => T): T = {
    val old = variableMap
    variableMap = variableMap ++ m
    val res = f
    variableMap = old
    res
  }

  // finalize source subset
  def finalize(e: Expr): target.Source = e match {

    case c:Cmp => source.cmp(c.op, finalize(c.e1).asInstanceOf[source.Rep], 
      finalize(c.e2).asInstanceOf[source.Rep]).asInstanceOf[target.Source]
    case p:Project => source.project(finalize(p.tuple).asInstanceOf[source.Rep], 
      p.field).asInstanceOf[target.Source]
    case v:VarRef => variableMap.getOrElse(v.varDef, source.vref(v.name, v.tp)).asInstanceOf[target.Source]
    case _ => sys.error(s"unsupported entry in source $e")
    
  }

  // finalize target subset
  def finalize(e: PExpr): target.Target = e match {

    case OuterJoin(e1, ve1, e2, ve2, cond, fs) => 
      target.outerjoin(finalize(e1), finalize(e2), 
        (r: target.Source, s: target.Source) => withMapList(List((ve1, r), (ve2, s)))(finalize(cond)), fs)

    case _ => sys.error(s"unsupported entry in target $e")

  }

}