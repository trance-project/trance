package shredding.nrc

import shredding.core._
import collection.mutable.{ HashMap => HMap }

class Context {
  val ctx: HMap[String, Any] = HMap(EmptyCtx.name -> EmptyCtx.value)

  def apply(n: String): Any = ctx(n)

  def add(n: String, v: Any, tp: Type): Unit = tp match {
    case TupleType(tas) if tas.contains("lbl") =>
      // Extract variables encapsulated by the label
      val m = v.asInstanceOf[Map[String, Map[String, Any]]]("lbl")
      val LabelType(las) = tas("lbl")
      las.foreach { case (n2, t2) => add(n2, m(n2), t2) }
      ctx(n) = v
    case _ => ctx(n) = v
  }

  def remove(n: String, tp: Type): Unit = tp match {
    case LabelType(as) =>
      // Remove variables encapsulated by the label
      as.foreach { case (n2, tp2) => remove(n2, tp2) }
    case _ => ctx.remove(n)
  }

  override def toString: String = ctx.toString
}

object EmptyCtx {
  val name = "emptyCtx"

  val tp = BagType(TupleType("lbl" -> LabelType()))

  val value = List(Map("lbl" -> Map.empty[String, Any]))
}
