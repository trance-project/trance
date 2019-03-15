package shredding.core

package object core extends Type with OpCmp with Variable{ 
  
  def quote(e: VarDef): String = e.name
  def quote(e: OpCmp): String = e.toString
    
}
