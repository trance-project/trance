package framework.runtime

import framework.core.VarDef

import scala.collection.mutable.{HashMap => HMap}

/**
  * Context used during evaluation
  */
class RuntimeContext(val ctx: HMap[VarDef, Any] = HMap()) extends ScalaRuntime {

  def apply(varDef: VarDef): Any = ctx(varDef)

  def contains(varDef: VarDef): Boolean = ctx.contains(varDef)

  def add(varDef: VarDef, v: Any): Unit = ctx(varDef) = v

  def remove(varDef: VarDef): Unit = ctx.remove(varDef)

  override def toString: String = ctx.toString
}

object RuntimeContext {
  def apply(values: (VarDef, Any)*): RuntimeContext = new RuntimeContext(HMap(values: _*))
}

