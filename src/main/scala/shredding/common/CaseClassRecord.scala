package shredding.core

trait CaseClassRecord {
  def uniqueId: Long
  override def equals(o: Any): Boolean = o match {
    case c:CaseClassRecord =>
      val field = c.getClass.getDeclaredFields.last
      field.setAccessible(true)
      field.get(c) == uniqueId
    case _ => false
  }
}
