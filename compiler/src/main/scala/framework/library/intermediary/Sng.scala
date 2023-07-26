package framework.library.intermediary

case class Sng[T](in: Rep[T]) extends WrappedDataframe[T]
