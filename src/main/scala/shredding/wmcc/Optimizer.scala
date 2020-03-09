package shredding.wmcc

import shredding.core._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

object Optimizer {

  val dictIndexer = new Finalizer(new BaseDictNameIndexer{})

  val compiler = new BaseNormalizer{}
  import compiler._

  val extensions = new Extensions{}
  import extensions._

  val proj = HashMap[Variable, Set[String]]().withDefaultValue(Set())
 
  def applyAll(e: CExpr) = {
    val projectionsPushed = push(e)
    val merged = mergeOps(projectionsPushed)
    dictIndexer.finalize(merged).asInstanceOf[CExpr]
  }

  def fields(e: CExpr):Unit = e match {
    case Record(ms) => ms.foreach(f => fields(f._2))
    case Sng(r) => fields(r)
    case Tuple(fs) => fs.foreach( f => fields(f))
    case Project(v @ Variable(_,_), s) => proj(v) = proj(v) ++ Set(s)
    case Gte(e1, e2) => fields(e1); fields(e2);
    case Equals(e1, e2) => fields(e1); fields(e2)
    case Multiply(e1, e2) => fields(e1); fields(e2);
    case And(e1, e2) => fields(e1); fields(e2);
    case _ => Unit
  }

  def printhm():Unit = proj.foreach(f => println(s"${f._1.asInstanceOf[Variable].name} -> ${f._2}")) 

  def mergeOps(e: CExpr): CExpr = fapply(e, {
    // cast domains
    case CNamed(n, CDeDup(r @ Reduce(Unnest(e1, _, _, _, _,_), v2, f2, p2))) if n.contains("M_ctx") =>
      CNamed(n, mergeOps(Reduce(e1, v2, f2, p2)))
    // recurse?
    case CNamed(n, CDeDup(r @ Reduce(e1, v2, f2, p2))) if n.contains("M_ctx") =>
      CNamed(n, mergeOps(Reduce(e1, v2, f2, p2)))
    case Reduce(Nest(Join(e1:Select, e2, e1s, key1, e2s, key2),
     vs, key, value, nv, np, ng), vr, fr, pr) => 
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, value)
    // analyze this case
    case Reduce(Nest(Lookup(e1:Select, e2, e1s, key1, e2s, key2, key3),
     vs, key, value, nv, np, ng), vr, fr, pr) => 
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, value)
    case Reduce(CoGroup(e1, e2, e1s, e2s, key1, key2, value), vr, fr, pr) => 
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, fr)
    // todo push record type projection into lookup
    // case Reduce(Nest(e1, vs, key, value, nv, np, ng), v2, e2, p2) => 
    //   Nest(mergeOps(e1), vs, key, value, nv, np, ng)
    case Reduce(Select(x, v, p, e2:Variable), v2, f2, p2) => 
      Reduce(x, List(v), f2, compiler.and(p, p2))
    case Reduce(Select(x, v, p, e2), v2, f2:Variable, p2) =>
      Reduce(x, List(v), e2, compiler.and(p, p2))
    case Reduce(Reduce(x, v, e2, p), v2, f2:Variable, p2) => 
      Reduce(x, v2, e2, compiler.and(p, p2))
    case Select(x, v, Constant(true), e2) => 
      Reduce(x, List(v), e2, Constant(true))
    case Select(x, v, p, e2) => 
      Reduce(x, List(v), e2, p)
  })
 
  def push(e: CExpr): CExpr = e match {
    case Reduce(Select(x, v, p, e2), v2, f2, p2) => x match {
      case InputRef(n, BagDictCType(flat, tdict)) =>
        push(Reduce(InputRef(n, flat), v2, f2, and(p, p2)))
      case _ => 
        fields(p)
        fields(p2)
        fields(f2)
        Reduce(push(Select(x, v, p, e2)), v2, f2, p2)
    }
    case Reduce(d, v, f, p) => 
      fields(f)
      fields(p)
      Reduce(push(d), v, f, p)
    case Nest(e1, v1, f, e, v, p, g) => 
      fields(e)
      fields(f)
      fields(p)
      Nest(push(e1), v1, f, e, v, p, g)
    case Unnest(e1, v1, f, v2, p, value) =>
      fields(f)
      Unnest(push(e1), v1, f, v2, p, value)
    case OuterUnnest(e1, v1, f, v2, p, value) =>
      fields(f)
      OuterUnnest(push(e1), v1, f, v2, p, value)
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
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      fields(p2)
      fields(p3)
      Lookup(e1, e2, v1, p1, v2, p2, p3)
    case CNamed(n, o) => CNamed(n, push(o))
    case LinearCSet(rs) => LinearCSet(rs.reverse.map(r => push(r)).reverse)
    case _ => e
  }

}
