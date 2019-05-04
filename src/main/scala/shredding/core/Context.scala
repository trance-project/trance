package shredding.core

import scala.collection.mutable.{HashMap => HMap}

class Context[A] {
  val ctx: HMap[String, A] = HMap()

  def apply(n: String): A = ctx(n)

  def add(n: String, v: A, tp: Type): Unit = tp match {
    case TupleType(tas) if tas.contains("lbl") =>
      // Extract variables encapsulated by the label
      val m = v.asInstanceOf[Map[String, Map[String, A]]]("lbl")
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

  def clear = ctx.clear

  override def toString: String = ctx.toString
}
