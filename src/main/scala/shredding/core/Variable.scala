package shredding.core

trait Variable

object VarCnt{
  var currId = 0
}

case class VarDef(n: String, tp: Type, nid: Int = { VarCnt.currId += 1; VarCnt.currId; }) extends Variable{
  def name: String = n+nid
}
