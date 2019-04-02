package shredding

object Utils {

  // Indent text by n*2 spaces (and trim trailing space)
  def ind(s: String, n: Int = 1): String = {
    val i = "  " * n
    i + s.replaceAll("\n? *$", "").replaceAll("\n", "\n" + i)
  }

  // Fresh variable name provider
  object Symbol {
    private val counter = scala.collection.mutable.HashMap[String, Int]()

    def fresh(name: String = "x"): String = {
      val c = counter.getOrElse(name, 0) + 1
      counter.put(name, c)
      name + c
    }

    def freshClear(): Unit = counter.clear

    def getId(name: String): Int = counter.getOrElse(name, 0)
  }
}