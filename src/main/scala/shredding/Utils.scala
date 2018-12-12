package shredding

object Utils {

  // Indent text by n*2 spaces (and trim trailing space)
  def ind(s: String, n: Int = 1): String = {
    val i = "  " * n
    i + s.replaceAll("\n? *$", "").replaceAll("\n", "\n" + i)
  }
}