package shredding.wmcc

import shredding.core._
import scala.collection.immutable.Set
import scala.collection.mutable.HashMap

object Optimizer {

  val dictIndexer = new Finalizer(new BaseDictNameIndexer{})

  val normalizer = new BaseNormalizer{}
  // import compiler._

  val extensions = new Extensions{}
  import extensions._

  val base = new BaseCompiler{}
  val compiler = new Finalizer(base)


  val proj = HashMap[Variable, Set[String]]().withDefaultValue(Set())

  def indexOnly(e: CExpr) = {
    dictIndexer.finalize(e).asInstanceOf[CExpr]
  }

  def projectOnly(e: CExpr) = {
    val projectionsPushed = push(e)
    val indexed = dictIndexer.finalize(projectionsPushed).asInstanceOf[CExpr]
    indexed
  }
 
  def applyAll(e: CExpr) = {
    val projectionsPushed = push(e)
    val merged = mergeOps(projectionsPushed)
    val indexed = dictIndexer.finalize(pushNest(merged)).asInstanceOf[CExpr]
    indexed
  }

  def fields(e: CExpr):Unit = e match {
    case Label(ms) => ms.foreach(f => fields(f._2))
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

  def collectVars(e: CExpr): Set[Variable] = e match {
    case Record(ms) => ms.foldLeft(Set.empty[Variable])((acc, m) => acc ++ collectVars(m._2))
    case Tuple(fs) => fs.foldLeft(Set.empty[Variable])((acc, m) => acc ++ collectVars(m))
    case Project(v, f) => collectVars(v)
    case v:Variable => Set(v)
    case _ => sys.error(s"implementation missing $e")
  }

  def pushNest(e: CExpr): CExpr = fapply(e, {
    // todo check value does not contain o values
    case Nest(OuterJoin(e1, e2, v1s, key1 @ Project(e1v, f1), v2, key2, proj1, proj2), 
      v2s, Tuple(gbs), gbval, v3, filt, Tuple(nulls), dk) if (gbs.contains(e1v) && !gbval.tp.isPrimitive) =>
        val pv = Variable.fresh(TTupleType(List(key2.tp, BagCType(gbval.tp))))
        val pushed = pushNest(Nest(e2, List(v2), key2, gbval, pv, filt, Tuple((nulls.toSet - e1v).toList), dk))
        OuterJoin(e1, pushed, v1s, key1, pv, Project(pv, "_1"), Tuple(v1s), Project(pv, "_2"))

    case Nest(Join(e1, e2, v1s, key1 @ Project(e1v, f1), v2, key2, proj1, proj2), 
      v2s, Tuple(gbs), gbval, v3, filt, Tuple(nulls), dk) if (gbs.contains(e1v) && !gbval.tp.isPrimitive) =>
        val pv = Variable.fresh(TTupleType(List(key2.tp, BagCType(gbval.tp))))
        val pushed = pushNest(Nest(e2, List(v2), key2, gbval, pv, filt, Tuple((nulls.toSet - e1v).toList), dk))
        Join(e1, pushed, v1s, key1, pv, Project(pv, "_1"), Tuple(v1s), Project(pv, "_2"))
    
    // primitive aggregation, move right
    // case Nest(Join(e1, e2, v1s, key1 @ Project(e1v, f1), v2, key2, proj1, proj2), 
    //   v2s, Tuple(gbs), gbval @ Project(e2v, f2), v3, filt, Tuple(nulls)) if e1v == e2v && gbval.tp.isPrimitive =>
    //     // this needs to remove additional attributes
    //     val parentKey = Tuple(v1s :+ key1)
    //     val aggVal = gbval
    //     val pv = Variable.fresh(TTupleType(List(key1.tp, gbval.tp)))
    //     // filter needs pushed as well
    //     val pushed = pushNest(Nest(e1, v1s, parentKey, gbval, pv, Constant(true), Tuple((nulls.toSet - v2).toList)))
    //     Nest(Join(pushed, e2, v1s, key1, v2, key2, proj1, proj2), 
    //       v2s, Tuple(gbs), gbval, v3, filt, Tuple(nulls))

    // case Nest(Lookup(lbl, dict, v1s, extract, v2, key1, p), v2s, Tuple(gbs), gbval, v3, filt, Tuple(nulls)) => 
    //   Lookup(lbl, dict, v1s, extract, v2, key1, 
    //     Comprehension(Project(dict, "_2"), v2, filt, Record(Map("key" -> Tuple(gbs), "agg" -> gbval))))
    
    })
  
  def wrapLabel(l: CExpr, nv: CExpr): CExpr = l match {
    case Label(ls) if ls.size == 1 => ls.head match {
      case (attr, p @ Project(_, _)) => 
        Label(Map(attr -> p))//Variable(v.name, p.tp)))
      case _ => ???
    }
    case _ => sys.error(s"not a label $l")
  }

  def mergeOps(e: CExpr): CExpr = fapply(e, {

    /** cast domains **/
    case CNamed(n, CDeDup(r @ Reduce(Unnest(e1, _, _, _, _,_), v2, f2, p2))) if n.contains("M_ctx") =>
      CNamed(n, mergeOps(Reduce(e1, v2, f2, p2)))
    case CNamed(n, CDeDup(r @ Reduce(e1, v2, f2, p2))) if n.contains("M_ctx") =>
      CNamed(n, mergeOps(Reduce(e1, v2, f2, p2)))
    
    /** wrap label type during group by label **/   
    case Reduce(Nest(e1, vs, Tuple(fs), value, nv, np, CUnit, dk), v2, Record(ms), p2) 
      if ms.keySet == Set("_1", "_2") && fs.size == 1 => ms get "_1" match {
        case Some(lbl) => 
          mergeOps(Nest(e1, vs, wrapLabel(lbl, fs.head), value, nv, np, CUnit, dk))
        case _ => e
      }
    case Reduce(r @ Reduce(x, v, Tuple(fs), Constant("null")), v2, Record(ms), p2) 
      if ms.keySet == Set("_1", "_2") && fs.size == 2 => ms get "_1" match {
        case Some(lbl) => 
          mergeOps(Reduce(x, v, Tuple(wrapLabel(lbl, fs.head) +: fs.tail), Constant("null")))
        case _ => e
      }

    /** push basic projections **/
    case Reduce(Select(x, v, p, e2), v2, f2:Variable, p2) =>
      Reduce(x, List(v), e2, normalizer.and(p, p2))
    case Reduce(Select(x, v, p, e2), v2, f2, p2) => 
      Reduce(x, List(v), f2, normalizer.and(p, p2))
    case Select(x, v, Constant(true), e2) => 
      Reduce(x, List(v), e2, Constant(true))
    case Select(x, v, p, e2) => 
      Reduce(x, List(v), e2, p)

    // unshredding, maybe this could be an unnesting case
    case Reduce(Nest(Lookup(e1, e2, e1s, key1, e2s, key2, key3), 
      vs, key, value, nv, np, ng, _), v3, Record(ms), filt) if !value.tp.isPrimitive && ms.keySet == Set("_1", "_2") => 
      Reduce(Lookup(mergeOps(e1), mergeOps(e2), e1s, key1, e2s, key2, key3), vs, Tuple(List(vs.head, value)), filt)

    /** merge nests and joins **/
    case Nest(OuterJoin(e1, e2, e1s, key1, e2s, key2, proj1, proj2), 
      vs, key, value, nv, np, ng, _) if (collectVars(value) == Set(e2s) && !value.tp.isPrimitive) =>
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, value)
    case Nest(Join(e1:Select, e2, e1s, key1, e2s, key2, proj1, proj2), 
      vs, key, value, nv, np, ng, _) if !value.tp.isPrimitive => 
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, value)
    case Nest(Lookup(e1:Select, e2, e1s, key1, e2s, key2, key3), 
      vs, key, value, nv, np, ng, _) if !value.tp.isPrimitive => 
      CoGroup(mergeOps(e1), mergeOps(e2), e1s, e2s, key1, key2, value)

    case Nest(OuterUnnest(e1, v1, lbag, v2, p, _), vs, key, value, v3, p2, _,_) 
      if e1.tp.isDict => 
      println("triggering this")
      println(e1.tp)
      Reduce(e1, v1, Tuple(List(key, Comprehension(lbag, v2, p, value))), p2)
  
    // case OuterJoin(Nest(e1, vs, key, value, nv, np, ng), e2, e1s, key1, e2s, key2, proj1, proj2) =>
    //   CoGroup(e1, mergeOps(e2), e1s, e2s, key1, key2, value)

    // /** merging basic map operations **/
    // case Reduce(CoGroup(e1, e2, e1s, e2s, key1, key2, value), vr, fr, pr) => 
    //   mergeOps(CoGroup(e1, mergeOps(e2), e1s, e2s, key1, key2, fr))
    // todo push record type projection into lookup

  })
 
  def push(e: CExpr): CExpr = e match {
    case Reduce(Select(x, v, p, e2), v2, f2, p2) => x match {
      case InputRef(n, BagDictCType(flat, tdict)) =>
        push(Reduce(InputRef(n, flat), v2, f2, normalizer.and(p, p2)))
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
    case Nest(e1, v1, f, e, v, p, g, dk) => 
      fields(e)
      fields(f)
      fields(p)
      Nest(push(e1), v1, f, e, v, p, g, dk)
    case Unnest(e1, v1, f, v2, p, value) =>
      fields(f)
      Unnest(push(e1), v1, f, v2, p, value)
    case OuterUnnest(e1, v1, f, v2, p, value) =>
      fields(f)
      OuterUnnest(push(e1), v1, f, v2, p, value)
    case Join(e1, e2, v1, p1, v2, p2, proj1, proj2) =>   
      fields(p1)
      fields(p2)
      fields(proj1)
      fields(proj2)
      Join(push(e1), push(e2), v1, p1, v2, p2, proj1, proj2)
    case OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2) =>   
      fields(p1)
      fields(p2)
      fields(proj1)
      fields(proj2)
      OuterJoin(push(e1), push(e2), v1, p1, v2, p2, proj1, proj2)
    //case Select(InputRef(n, _), v,_,_) if n.contains("M_ctx") => e
    case Select(d, v, f, e2) =>
      fields(e)
      val projs = proj(v)
      e2 match {
        case Variable(_, RecordCType(tfs)) if tfs.keySet != projs => // && tfs.keySet != Set("lbl") =>
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
