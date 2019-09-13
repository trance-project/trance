package shredding.wmcc

import shredding.core._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

object Projections {

  // temporary hack to handle missing label projections
  val tmp1 = HashMap[String, Set[String]]().withDefaultValue(Set())
  val tmp2 = HashMap[Variable, Set[String]]().withDefaultValue(Set())

  val proj = HashMap[Variable, Set[String]]().withDefaultValue(Set())
  
  def fields(e: CExpr):Unit = e match {
    case Record(ms) => ms.foreach(f => { 
      if (tmp1.contains(f._1) && f._2.isInstanceOf[Variable]){
        tmp2(f._2.asInstanceOf[Variable]) = tmp1(f._1)
      }
      fields(f._2)
    })
    case Project(Project(Project(_, "lbl"), s1), s2) => tmp1(s1) = Set(s2)
    case Project(v @ Variable(_,_), s) => 
      proj(v) = proj(v) ++ Set(s) ++ tmp2(v) 
    case _ => Unit//sys.error(s"unsupported $e")
  }

  def printhm():Unit = proj.foreach(f => println(s"${f._1.asInstanceOf[Variable].name} -> ${f._2}"))
  
  def push(e: CExpr): CExpr = e match {
    case Reduce(d, v, f, p) => 
      fields(f)
      Reduce(push(d), v, f, p)
    case Nest(e1, v1, f, e, v, p, g) => 
      fields(e)
      Nest(push(e1), v1, f, e, v, p, g)
    case Unnest(e1, v1, f, v2, p) =>
      fields(f) // TODO
      Unnest(push(e1), v1, f, v2, p)
    case OuterUnnest(e1, v1, f, v2, p) =>
      fields(f) //TODO
      OuterUnnest(push(e1), v1, f, v2, p)
    case Join(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      Join(push(e1), push(e2), v1, p1, v2, p2)
    case OuterJoin(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      OuterJoin(push(e1), push(e2), v1, p1, v2, p2)
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      fields(p2)
      fields(p3)
      Lookup(push(e1), push(e2), v1, p1, v2, p2, p3)
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) =>
      fields(p2)
      fields(p3)
      OuterLookup(push(e1), push(e2), v1, p1, v2, p2, p3)
    case Select(InputRef(n, _), _,_,_) if n.contains("M_ctx") => e 
    case Select(d, v, f, e2) =>
      fields(e)
      Select(push(d), v, f, Record((proj(v) ++ tmp2(v)).map(f2 => f2 -> Project(v, f2)).toMap))
    case CNamed(n, o) => CNamed(n, push(o))
    case LinearCSet(rs) => LinearCSet(rs.reverse.map(r => push(r)).reverse)
    case _ => e
  } 

}
