package shredding.core

import shredding.Utils.ind

package object core extends Type with OpCmp with Variable with BaseDict with Label{ 
  
  def quote(e: VarDef): String = e.name
  def quote(e: OpCmp): String = e.toString

}
