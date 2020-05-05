package framework.core

/**
  * This checks standard equality
  */
case class Rec(map: Map[String, Any]) {
  override def toString(): String = map.map(x => s"${x._1}:${x._2}").mkString("Rec(", ",", ")")
}

object Rec {
  def apply(vs: (String, Any)*): Rec = Rec(vs.toMap)
}


trait CaseClassRecord { self =>
  def uniqueId: Long
  override def equals(o: Any): Boolean = o match {
    case c:CaseClassRecord =>
      val field = c.getClass.getDeclaredFields.last
      field.setAccessible(true)
      field.get(c) == uniqueId
    case _ => false
  }
  def toRec: Rec = {
    val m = (Map[String, Any]() /: self.getClass.getDeclaredFields) {(a, f) =>
      f.setAccessible(true)
      a + (f.getName -> f.get(self))
    } - "uniqueId"
    Rec(m)
  }
}

/**
  * This is used for dot-equality
  */
case class RecordValue(map: Map[String, Any], uniqueId: Long) extends CaseClassRecord {
  override def toString(): String = map.map(x => s"${x._1}:${x._2}").mkString("RecV(", ",", ")")
}

object RecordValue {
  def apply(vs: Map[String,Any]): RecordValue = RecordValue(vs, newId)
  def apply(vs: (String, Any)*): RecordValue = RecordValue(vs.toMap, newId)
  var id = 0L
  def newId: Long = {
    val prevId = id
    id += 1
    prevId
  }
}
