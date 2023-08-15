package framework.library.utilities

object Symbol {
  private val counter = scala.collection.mutable.HashMap[String, Int]()

  def fresh(name: String = "s"): String = {
    val c = counter.getOrElse(name, 0) + 1
    counter.put(name, c)
    name + c
  }

  def freshClear(): Unit = counter.clear

  def getId(name: String): Int = counter.getOrElse(name, 0)
}