package framework.common

/**
  * Variable definition
  */
final case class VarDef(name: String, tp: Type) {

  override def equals(that: Any): Boolean = that match {
    case that: VarDef => this.name == that.name && this.tp == that.tp
    case _ => false
  }

  def batch(tp: Type): VarDef = tp match {
    case BagType(tup) => 
      val ts = (tp.attrs ++ tup.attrs).asInstanceOf[Map[String, TupleAttributeType]]
      VarDef(name, TupleType(ts))
    case _ => sys.error(s"unsupported type $tp")
  }

  def keys: Set[String] = tp match {
    case TupleType(fs) => fs.keySet
    case _ => sys.error(s"unsupported type $tp")
  }

  override def hashCode: Int = (name, tp).hashCode
}
