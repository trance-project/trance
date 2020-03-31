package shredding.wmcc

import shredding.core._
import shredding.utils.Utils.ind

/**
  * Top down printer (preserves variable names through transformations)
  * for bottom up printer see BaseStringify in Base.scala
  */

object Printer {
  val compiler = new BaseStringify{}
  import compiler._
  def quote(e: CExpr): String = e match {
    case InputRef(d, t) => inputref(d,t)
    case Input(d) => input(d.map(quote(_)))
    case Constant(d) => constant(d)
    case EmptySng => emptysng
    case CUnit => unit
    case Index => "index"
    case Sng(e1) => sng(quote(e1))
    case Tuple(fs) => tuple(fs.map(quote(_)))
    case Record(fs) => record(fs.map(f => f._1 -> quote(f._2)))
    case Label(fs) => label(fs.map(f => f._1 -> quote(f._2)))
    case Multiply(e1, e2) => compiler.mult(quote(e1), quote(e2))
    case Equals(e1, e2) => compiler.equals(quote(e1), quote(e2))
    case Lt(e1, e2) => lt(quote(e1), quote(e2))
    case Lte(e1, e2) => lte(quote(e1), quote(e2))
    case Gt(e1, e2) => gt(quote(e1), quote(e2))
    case Gte(e1, e2) => gte(quote(e1), quote(e2))
    case And(e1, e2) => and(quote(e1), quote(e2))
    case Not(e1) => not(quote(e1))
    case Or(e1, e2) => or(quote(e1), quote(e2))
    case Project(e1, f) => project(quote(e1), f)
    case If(c, e1, e2) => e2 match {
      case Some(a) => ifthen(quote(c), quote(e1), Some(quote(a)))
      case _ => ifthen(quote(c), quote(e1), None)
    }
    case Merge(e1, e2) => merge(quote(e1), quote(e2))
    case Comprehension(e1, v, p, e) => p match {
      case Constant(true) => s"{ ${quote(e)} | ${v.quote} <- ${quote(e1)} }"
      case px => s"{ ${quote(e)} | ${v.quote} <- ${quote(e1)}, ${quote(px)} }"
    }
    case Bind(x, e1, e2) => s"{ ${quote(e2)} | ${quote(x)} := ${quote(e1)} }"
    case CDeDup(e1) => s"DeDup(${quote(e1)})"
    case CGroupBy(e1, v, grp, value) => s"(${quote(e1)}).groupBy(${grp.mkString(",")}, ${value.mkString(",")})"
    case CReduceBy(e1, v, grp, value) => s"(${quote(e1)}).reduceBy(${grp.mkString(",")}, ${value.mkString(",")})"
    case CNamed(n, e) => named(n, quote(e))
    case LinearCSet(exprs) => linset(exprs.map(quote(_)))
    case CLookup(lbl, dict) => lookup(quote(lbl), quote(dict))
    case EmptyCDict => emptydict
    case BagCDict(lblTp, flat, dict) => bagdict(lblTp, quote(flat), quote(dict))
    case TupleCDict(fs) => tupledict(fs.map(f => f._1 -> quote(f._2)))
    case DictCUnion(e1, e2) => dictunion(quote(e1), quote(e2))
    case Select(x, v, p, e) => 
      s"""| <-- (${quote(v)}) -- SELECT[ ${quote(p)}, ${quote(e)} ](${quote(x)})""".stripMargin
    case Reduce(e1, v, e2:Variable, Constant(true)) =>
      s""" | ${quote(e1)}""".stripMargin
    case Reduce(e1, v, e2, Constant(true)) =>
      s""" | PROJECT[ ${quote(e2)} ](${quote(e1)})""".stripMargin
    case Reduce(e1, v, e2, p) =>
      s""" | SELECT[ ${quote(e2)} / ${quote(p)} ](${quote(e1)})""".stripMargin
    case Unnest(e1, v1, e2, v2, p, value) =>
      s""" |  <-- (${e.wvars.map(_.quote).mkString(",")}) --
           | UNNEST[ ${quote(e2)} / ${quote(p)} / ${quote(value)} ](${quote(e1)})""".stripMargin
    case OuterUnnest(e1, v1, e2, v2, p, value) =>
      s""" |  <-- (${e.wvars.map(_.quote).mkString(",")}) -- 
           | OUTERUNNEST[ ${quote(e2)} / ${quote(p)} / ${quote(value)}  ](${quote(e1)})""".stripMargin
    case Nest(e1, v1, f, e3, v2, p, g, dk) =>
      val acc = e3.tp match { case IntType => "+"; case _ => "U" }
      //val outs = if (e.wvars.size == 1) "" else s"<-- (${e.wvars.map(_.quote).mkString(",")}) --"
      s""" | NEST[ ${acc} / ${quote(e3)} / ${quote(f)}, ${quote(p)} / ${quote(g)} ](${quote(e1)})""".stripMargin
    case Join(e1, e2, v1, p1, v2, p2, proj1, proj2) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) JOIN[${quote(p1)} = ${quote(p2)}](
           | ${ind(quote(e2))})""".stripMargin
    case OuterJoin(e1, e2, v1, p1, v2, p2, proj1, proj2) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) OUTERJOIN[${quote(p1)} = ${quote(p2)}](
           | ${ind(quote(e2))})""".stripMargin
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) LOOKUP[${quote(p1)}, ${quote(p2)} = ${quote(p3)}](
           | ${ind(quote(e2))})""".stripMargin
    case OuterLookup(e1, e2, v1, p1, v2, p2, p3) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) OUTERLOOKUP[${quote(p1)}, ${quote(p2)} = ${quote(p3)}](
           | ${ind(quote(e2))})""".stripMargin
    case CoGroup(e1, e2, vs, v, k1, k2, value) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) COGROUP[${quote(k1)} = ${quote(k2)}, 
           | ${quote(value)}] (
           |  ${ind(quote(e2))})""".stripMargin
    case FlatDict(e1) => flatdict(quote(e1))
    case GroupDict(e1) => groupdict(quote(e1))
    case Variable(n, tp) => n
  }
}
