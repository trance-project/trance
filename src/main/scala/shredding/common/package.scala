package shredding.core

package object core extends OpCmp {

  def quote(e: VarDef): String = e.name

  def quote(e: OpCmp): String = e.toString
}

