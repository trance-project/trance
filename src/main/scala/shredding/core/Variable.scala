package shredding.core

trait Variable

object VarCnt{
  var currId = 0
  def reset = currId = 0
  def inc = { currId += 1; currId }
}

case class VarDef(n: String, tp: Type, nid: Int = 0) extends Variable {
  def name: String = n+nid
}
