package shredding

import collection.mutable.Map

object Utils {

  // Indent text by n*2 spaces (and trim trailing space)
  def ind(s: String, n: Int = 1): String = {
    val i = "  " * n
    i + s.replaceAll("\n? *$", "").replaceAll("\n", "\n" + i)
  }

  // if element to add in map is there, then append to value
  // deprecated 
  def merge(m: Map[Any, Any], m2: Map[_, _]) = {
    m2.foreach(kv => {
      m.get(kv._1.asInstanceOf[Any]) match {
        case Some(e) => m(kv._1) = m(kv._1.asInstanceOf[Any]).asInstanceOf[List[Any]] :+ kv._2
        case None => m += kv
      }
    })
  }

  // list[Any].flatten
  def flatten(ls: List[Any]): List[Any] = ls flatMap {
    case i: List[_] => flatten(i)
    case i => List(i)
  }

}
