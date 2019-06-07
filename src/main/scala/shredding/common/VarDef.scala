package shredding.core

/**
  * Variable definition
  */
case class VarDef(name: String, tp: Type) { self =>

  override def equals(that: Any): Boolean = that match {
    case that: VarDef => this.name == that.name && this.tp == that.tp
    case _ => false
  }

  override def hashCode: Int = (name, tp).hashCode()
  def quote: String = self.name
}

object VarDef {
  private var lastId = 1
  def fresh(tp: Type): VarDef = {
    val id = lastId
    lastId += 1
    VarDef(s"x$id", tp)
  }
}

