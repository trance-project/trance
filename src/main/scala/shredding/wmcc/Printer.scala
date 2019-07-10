package shredding.wmcc

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
    case Sng(e1) => sng(quote(e1))
    case WeightedSng(e1, qt) => weightedsng(quote(e1), quote(qt))
    case Tuple(fs) => tuple(fs.map(quote(_)))
    case Record(fs) => record(fs.map(f => f._1 -> quote(f._2)))
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
    case CNamed(n, e) => named(n, quote(e))
    case LinearCSet(exprs) => linset(exprs.map(quote(_)))
    case CLookup(lbl, dict) => lookup(quote(lbl), quote(dict))
    case EmptyCDict => emptydict
    case BagCDict(lbl, flat, dict) => bagdict(quote(lbl), quote(flat), quote(dict))
    case TupleCDict(fs) => tupledict(fs.map(f => f._1 -> quote(f._2)))
    case DictCUnion(e1, e2) => dictunion(quote(e1), quote(e2))
    case Select(x, v, p) => quote(p) match {
      case "true" => quote(x)
      case _ => s""" 
        | <-- (${e.wvars.map(_.quote).mkString(",")})} -- SELECT[ ${quote(p)} ](${quote(x)})""".stripMargin
    }
    case Reduce(e1, v, e2, p) =>
      s""" | REDUCE[ ${quote(e2)} / ${quote(p)} ](${quote(e1)})""".stripMargin
    case Unnest(e1, v1, e2, v2, p) =>
      s""" |  <-- (${e.wvars.map(_.quote).mkString(",")}) -- UNNEST[ ${quote(e2)} / ${quote(p)} ](${quote(e1)})""".stripMargin
    case OuterUnnest(e1, v1, e2, v2, p) =>
      s""" |  <-- (${e.wvars.map(_.quote).mkString(",")}) -- OUTERUNNEST[ ${quote(e2)} / ${quote(p)} ](${quote(e1)})""".stripMargin
    case Nest(e1, v1, f, e3, v2, p) =>
      val acc = e match { case Constant(1) => "+"; case _ => "U" }
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- NEST[ ${acc} / ${quote(e3)} / ${quote(f)}, ${quote(p)} ](${quote(e1)})""".stripMargin
    case NestBlock(e1, v1, f, e3, v2, p, e2, v3) =>
      val acc = e match { case Constant(1) => "+"; case _ => "U" }
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- NEST[ ${acc} / ${quote(e3)} / ${quote(f)}, ${quote(p)} ](${quote(e2)})""".stripMargin
    case Join(e1, e2, v1, p1, v2, p2) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) JOIN[${quote(p1)} = ${quote(p2)}](
           | ${ind(quote(e2))})""".stripMargin
    case OuterJoin(e1, e2, v1, p1, v2, p2) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) OUTERJOIN[${quote(p1)} = ${quote(p2)}](
           | ${ind(quote(e2))})""".stripMargin
    case Lookup(e1, e2, v1, p1, v2, p2, p3) =>
      s""" | <-- (${e.wvars.map(_.quote).mkString(",")}) -- (${quote(e1)}) LOOKUP[${quote(p1)}, ${quote(p2)} = ${quote(p3)}](
           | ${ind(quote(e2))})""".stripMargin
    case Variable(n, tp) => n
  }
}
