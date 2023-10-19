package uk.ac.ox.cs.trance

/**
 * [[Rep]] is the base trait for the intermediary representation between Spark and NRC.
 * <br>
 * Everything in this representation is a [[Rep]] or extends it.
 * <br>
 * This layer allows a user to build a nested sequence of [[Rep]] that is then the recursively converted into an NRC Expression in [[NRCConverter]]
 */
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

/**
 * Used to represent a function argument in [[FlatMap]] & [[Map]]
 */
case class Fun[T1, T2](in: Rep[T1], out: Rep[T2]) extends Rep[T1 => T2]

case class Sng[T](in: Rep[T]) extends WrappedDataframe[T]

case class Sym[T](name: String) extends Rep[T]

