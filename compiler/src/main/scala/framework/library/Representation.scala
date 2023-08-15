package framework.library

case class Fun[T1, T2](in: Rep[T1], out: Rep[T2]) extends Rep[T1 => T2]

trait Rep[T] {
  //  def *(e2: Rep[T]): Rep[T] = Mult[T](this, e2)
}

case class Sng[T](in: Rep[T]) extends WrappedDataframe[T]

case class Sym[T](name: String) extends Rep[T]
