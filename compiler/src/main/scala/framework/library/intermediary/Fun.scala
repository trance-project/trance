package framework.library.intermediary

case class Fun[T1, T2](in: Rep[T1], out: Rep[T2]) extends Rep[T1 => T2]
