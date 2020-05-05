package framework.runtime

import framework.utils.Utils.ind
import framework.core._

/**
  * Printer of runtime Scala values
  */
trait ScalaPrinter {
  this: ScalaShredding =>

  def quote(e: ShredValue): String =
    s"""|Flat: ${e.flat}
        |Dict: ${quote(e.dict)}""".stripMargin

  def quote(d: RDict): String = d match {
    case REmptyDict => "Nil"
    case RInBagDict(f, _, dict) =>
      s"""|(
          |  f :=
          |${ind(f.toString, 2)},
          |  tupleDict :=
          |${ind(quote(dict), 2)}
          |)""".stripMargin
    case RTupleDict(fs) =>
      s"(${fs.map { case (k, v) => k + " := " + quote(v) }.mkString(", ")})"
    case _ => sys.error("Unknown runtime dictionary " + d)
  }

  def quote(v: Any, tp: Type): String = tp match {
    case StringType => "\"" + v.toString + "\""
    case _: PrimitiveType => v.toString
    case BagType(tp2) =>
      val l = v.asInstanceOf[List[Any]]
      s"""|[
          |${ind(l.map(quote(_, tp2)).mkString(",\n"))}
          |]""".stripMargin
    case TupleType(as) =>
      val m = v.asInstanceOf[Map[String, Any]]
      s"(${m.map { case (n, a) => n + " := " + quote(a, as(n)) }.mkString(", ")})"
    case _ => sys.error("Cannot print value of unknown type " + tp)
  }

}
