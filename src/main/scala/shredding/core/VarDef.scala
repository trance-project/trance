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
  override def toString: String = name
  def quote: String = self.toString
}

