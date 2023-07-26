package framework.library.intermediary
trait Rep[T] {
  def *(e2: Rep[T]): Rep[T] = Mult[T](this, e2)
}