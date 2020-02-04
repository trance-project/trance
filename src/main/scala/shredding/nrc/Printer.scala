package shredding.nrc

import shredding.core._

/**
  * Print of NRC expressions
  */
trait Printer {
  this: MaterializeNRC with BaseShredding =>

  import shredding.utils.Utils.ind

  def quote(e: Expr): String = e match {
    case c: Const if c.tp == StringType => "\"" + c.v + "\""
    case c: Const => c.v.toString
    case v: VarRef => v.name
    case p: Project => quote(p.tuple) + "." + p.field
    case ForeachUnion(x, e1, e2) =>
      s"""|For ${x.name} in ${quote(e1)} Union
          |${ind(quote(e2))}""".stripMargin
    case Union(e1, e2) =>
      s"""|(${quote(e1)})
          |Union
          |(${quote(e2)})""".stripMargin
    case Singleton(e1) => s"Sng(${quote(e1)})"
    case Tuple(fs) =>
      s"(${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(", ")})"
    case l: Let =>
      s"""|Let ${l.x.name} = ${quote(l.e1)} In
          |${quote(l.e2)}""".stripMargin
    case Total(e1) => s"Total(${quote(e1)})"
    case DeDup(e1) => s"DeDup(${quote(e1)})"
    case c: CondExpr => c match {
      case cmp: Cmp =>
        s"${quote(cmp.e1)} ${cmp.op} ${quote(cmp.e2)}"
      case And(e1, e2) =>
        s"${quote(e1)} AND ${quote(e2)}"
      case Or(e1, e2) =>
        s"${quote(e1)} OR ${quote(e2)}"
      case Not(e1) =>
        s"NOT ${quote(e1)}"
    }
    case i: IfThenElse =>
      if (i.e2.isDefined)
        s"""|If (${quote(i.cond)})
            |Then ${quote(i.e1)}
            |Else ${quote(i.e2.get)}""".stripMargin
      else
        s"""|If (${quote(i.cond)})
            |Then ${quote(i.e1)}""".stripMargin
    case ArithmeticExpr(op, e1, e2) =>
      s"(${quote(e1)} $op ${quote(e2)})"
    // Label extensions
    case x: ExtractLabel =>
      val tuple = x.lbl.tp.attrTps.keys.mkString(", ")
      s"""|Extract ${quote(x.lbl)} as ($tuple) In
          |${quote(x.e)}""".stripMargin
    case l: NewLabel =>
      s"NewLabel(${(l.id :: l.params.map(_.name).toList).mkString(", ")})"
    // Dictionary extensions
    case EmptyDict => "Nil"
    case BagDict(lbl, flat, dict) =>
      s"""|(${quote(lbl)} ->
          |  flat :=
          |${ind(quote(flat), 2)},
          |  tupleDict :=
          |${ind(quote(dict), 2)}
          |)""".stripMargin
    case TupleDict(fs) =>
      s"(${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(", ")})"
    case BagDictProject(v, f) => quote(v) + "." + f
    case TupleDictProject(v) => quote(v) + ".tupleDict"
    case d: DictUnion =>
      s"""|(${quote(d.dict1)})
          |DictUnion
          |(${quote(d.dict2)})""".stripMargin
    // Shredding extensions
    case ShredUnion(e1, e2) =>
      s"""|(${quote(e1)})
          |ShredUnion
          |(${quote(e2)})""".stripMargin
    case Lookup(lbl, dict) =>
      s"Lookup(lbl := ${quote(lbl)}, dict := ${quote(dict)})"
    case MatDictLookup(lbl, bag) =>
      s"MatDictLookup(lbl := ${quote(lbl)}, dict := ${quote(bag)})"

    /////////////////
    //
    //
    // UNSTABLE BELOW
    //
    //
    /////////////////


    case WeightedSingleton(e1, w1) =>
      s"WeightedSng(${quote(e1)}, ${quote(w1)})"

    case g: GroupBy => g.value.tp match {
      case b: BagType => s"(${quote(g.bag)}).groupBy(${quote(g.grp)}), ${quote(g.value)})"
      case _ => s"(${quote(g.bag)}).groupBy+(${quote(g.grp)}, ${quote(g.value)})"
    }

    case _ => sys.error("Cannot print unknown expression " + e)
  }

  def quote(a: Assignment): String = s"${a.name} := ${quote(a.rhs)}"

  def quote(p: Program): String = p.statements.map(quote).mkString("\n")

  def quote(e: ShredExpr): String =
    s"""|Flat: ${quote(e.flat)}
        |Dict: ${quote(e.dict)}""".stripMargin

  def quote(a: ShredAssignment): String =
    s"""|${flatName(a.name)} := ${quote(a.rhs.flat)}
        |${dictName(a.name)} := ${quote(a.rhs.dict)}""".stripMargin

  def quote(p: ShredProgram): String = p.statements.map(quote).mkString("\n")

}
