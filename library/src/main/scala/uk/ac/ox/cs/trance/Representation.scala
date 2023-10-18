package uk.ac.ox.cs.trance

import scala.language.implicitConversions

case class Fun[T1, T2](in: Rep[T1], out: Rep[T2]) extends Rep[T1 => T2]

trait Rep[T] {

    def *(e2: Rep[T]): Rep[T] = Mult[T](this, e2)
    def *(e2: Number): Rep[T] = Mult[T](this, Literal(e2))

    def +(e2: Rep[T]): Rep[T] = Add[T](this, e2)
    def +(e2: Number): Rep[T] = Add[T](this, Literal(e2))

    def -(e2: Rep[T]): Rep[T] = Sub(this, e2)
    def -(e2: Number): Rep[T] = Sub(this, Literal(e2))

    def /(e2: Rep[T]): Rep[T] = Divide(e2, this)
    def /(e2: Number): Rep[T] = Divide(Literal(e2), this)

    def %(e2: Rep[T]): Rep[T] = Mod(e2, this)
    def %(e2: Number): Rep[T] = Mod(Literal(e2), this)

    def === (e2: Rep[T]): Rep[T] = Equality[T](this, e2)
    def === (e2: Any): Rep[T] = Equality[T](this, Literal(e2))

    def =!=(e2: Rep[T]): Rep[T] = Inequality(this, e2)
    def =!=(e2: Any): Rep[T] = Inequality(this, Literal(e2))

    def >(e2: Rep[T]): Rep[T] = GreaterThan(this, e2)
    def >(e2: Number): Rep[T] = GreaterThan(this, Literal(e2))

    def >=(e2: Rep[T]): Rep[T] = GreaterThanOrEqual(this, e2)
    def >=(e2: Number): Rep[T] = GreaterThanOrEqual(this, Literal(e2))

    def <(e2: Rep[T]): Rep[T] = LessThan(e2, this)
    def <(e2: Number): Rep[T] = LessThan(Literal(e2), this)

    def <=(e2: Rep[T]): Rep[T] = LessThanOrEqual(e2, this)
    def <=(e2: Number): Rep[T] = LessThanOrEqual(Literal(e2), this)

    def &&(e2: Rep[T]): Rep[T] = AndRep(this, e2)

    def ||(e2: Rep[T]): Rep[T] = OrRep(this, e2)


}

case class Sng[T](in: Rep[T]) extends WrappedDataframe[T]

case class Sym[T](name: String) extends Rep[T]

