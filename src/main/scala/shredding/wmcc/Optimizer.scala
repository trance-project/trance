package shredding.wmcc

import shredding.core._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

object Optimizer {
  val dictIndexer = new Finalizer(new BaseDictNameIndexer{})
  val compiler = new BaseNormalizer{}
  import compiler._

  val proj = HashMap[Variable, Set[String]]().withDefaultValue(Set())
 
  def applyAll(e: CExpr) = push(dictIndexer.finalize(e).asInstanceOf[CExpr])

  def fields(e: CExpr):Unit = e match {
    case Record(ms) => ms.foreach(f => fields(f._2))
    case Sng(r) => fields(r)
    case Tuple(fs) => fs.foreach( f => fields(f))
    case Project(v @ Variable(_,_), s) => proj(v) = proj(v) ++ Set(s)
    case _ => Unit
  }

  def printhm():Unit = proj.foreach(f => println(s"${f._1.asInstanceOf[Variable].name} -> ${f._2}"))  


  def push(e: CExpr): CExpr = e match {
    case Reduce(Select(x, v, p, e2), v2, f2, p2) => x match {
      case InputRef(n, BagDictCType(flat, tdict)) =>
        Reduce(InputRef(n, flat), v2, f2, and(p, p2))
      case _ => Reduce(x, v2, f2, and(p, p2))
    }
    case Reduce(d, v, f, p) => 
      fields(f)
      Reduce(push(d), v, f, p)
    case Nest(e1, v1, f, e, v, p, g) => 
      fields(e)
      fields(f)
      Nest(push(e1), v1, f, e, v, p, g)
    case Unnest(e1, v1, f, v2, p) =>
      fields(f)
      Unnest(push(e1), v1, f, v2, p)
    case OuterUnnest(e1, v1, f, v2, p) =>
      fields(f)
      OuterUnnest(push(e1), v1, f, v2, p)
    case Join(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      Join(push(e1), push(e2), v1, p1, v2, p2)
    case OuterJoin(e1, e2, v1, p1, v2, p2) =>   
      fields(p1)
      fields(p2)
      OuterJoin(push(e1), push(e2), v1, p1, v2, p2)
    case Select(InputRef(n, _), v,_,_) if n.contains("M_ctx") => e
    case Select(d, v, f, e2) =>
      fields(e)
      val projs = proj(v)
      e2 match {
        case Variable(_, RecordCType(tfs)) if tfs.keySet != projs  && tfs.keySet != Set("lbl") =>
          Select(push(d), v, f, Record(projs.map(f2 => f2 -> Project(v, f2)).toMap))
        case _ =>
          Select(push(d), v, f, e2)
      }
    case CNamed(n, o) => CNamed(n, push(o))
    case LinearCSet(rs) => LinearCSet(rs.reverse.map(r => push(r)).reverse)
    case _ => e
  }

}
