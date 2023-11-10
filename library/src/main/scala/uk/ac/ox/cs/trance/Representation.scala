package uk.ac.ox.cs.trance

/**
 * [[Rep]] is the base trait for the intermediary representation between Spark and NRC.
 * <br>
 * Everything in this representation is a [[Rep]] or extends it.
 * <br>
 * This layer allows a user to build a nested sequence of [[Rep]] that is then the recursively converted into an NRC Expression in [[NRCConverter]]
 */
trait Rep {

    def *(e2: Rep): Rep = Mult(this, e2)
    def *(e2: Number): Rep = Mult(this, Literal(e2))

    def +(e2: Rep): Rep = Add(this, e2)
    def +(e2: Number): Rep = Add(this, Literal(e2))

    def -(e2: Rep): Rep = Sub(this, e2)
    def -(e2: Number): Rep = Sub(this, Literal(e2))

    def /(e2: Rep): Rep = Divide(e2, this)
    def /(e2: Number): Rep = Divide(Literal(e2), this)

    def %(e2: Rep): Rep = Mod(e2, this)
    def %(e2: Number): Rep = Mod(Literal(e2), this)

    def === (e2: Rep): Rep = Equality(this, e2)
    def === (e2: Any): Rep = Equality(this, Literal(e2))

    def =!=(e2: Rep): Rep = Inequality(this, e2)
    def =!=(e2: Any): Rep = Inequality(this, Literal(e2))

    def >(e2: Rep): Rep = GreaterThan(this, e2)
    def >(e2: Number): Rep = GreaterThan(this, Literal(e2))

    def >=(e2: Rep): Rep = GreaterThanOrEqual(this, e2)
    def >=(e2: Number): Rep = GreaterThanOrEqual(this, Literal(e2))

    def <(e2: Rep): Rep = LessThan(e2, this)
    def <(e2: Number): Rep = LessThan(Literal(e2), this)

    def <=(e2: Rep): Rep = LessThanOrEqual(e2, this)
    def <=(e2: Number): Rep = LessThanOrEqual(Literal(e2), this)

    def &&(e2: Rep): Rep = AndRep(this, e2)

    def ||(e2: Rep): Rep = OrRep(this, e2)


}

/**
 * Used to represent a function argument in [[FlatMap]] & [[Map]]
 */
case class Fun(in: Rep, out: Rep) extends Rep

case class Sng(in: Rep) extends WrappedDataframe

case class Sym(name: String) extends Rep

