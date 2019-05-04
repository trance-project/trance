package shredding.runtime

import shredding.core.{LabelType, TupleType, VarDef}

import scala.collection.mutable.{HashMap => HMap}

/**
  * Context used during evaluation
  */
class Context extends ScalaRuntime {

  val ctx: HMap[VarDef, Any] = HMap()

  def apply(varDef: VarDef): Any = ctx(varDef)

  def add(varDef: VarDef, v: Any): Unit = varDef.tp match {
    case TupleType(tas) if tas.contains("lbl") =>
      // Extract variables encapsulated by the label
      v.asInstanceOf[Map[String, RLabel]]("lbl") match {
        case ROutLabel(fs) =>
          val LabelType(las) = tas("lbl")
          las.foreach { case (n2, t2) =>
            val d = VarDef(n2, t2)
            add(d, fs(d))
          }
        case _ =>
      }
      ctx(varDef) = v
    case _ => ctx(varDef) = v
  }

  def remove(varDef: VarDef): Unit = varDef.tp match {
    case LabelType(as) =>
      // Remove variables encapsulated by the label
      as.foreach { case (n2, tp2) => remove(VarDef(n2, tp2)) }
      ctx.remove(varDef)
    case _ => ctx.remove(varDef)
  }

  override def toString: String = ctx.toString
}
