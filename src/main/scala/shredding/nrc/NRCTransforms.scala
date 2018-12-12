package shredding.nrc

import shredding.Utils.ind
import reflect.runtime.universe.{ Symbol => _, _ }

/**
  * Transformations for NRC expressions 
  * printing, evaluation, and shredding
  */

trait NRCTransforms extends NRCExprs {

  /**
    * Pretty printer
    */
  object Printer {
    def quote[A](e: Expr[A]): String = e match {
      case ForeachUnion(x, e1, e2) =>
        s"""|For ${quote(x)} in ${quote(e1)} Union
            |${ind(quote(e2))}""".stripMargin
      case Union(e1, e2) => s"(${quote(e1)}) Union (${quote(e2)})"
      case Singleton(e1) => "sng(" + quote(e1) + ")"
      case TupleStruct1(e1) => quote(e1)
      case TupleStruct2(e1, e2) => s"( ${quote(e1)}, ${quote(e2)} )"
      case TupleStruct3(e1, e2, e3) => s"( ${quote(e1)}, ${quote(e2)}, ${quote(e3)} )"
      case Project(e1, pos) => quote(e1) + "._" + pos
      case Relation(n, _) => quote(n)
      case Const(s: String) => "\""+ s +"\""
      case Const(c) => c.toString
      case Sym(x, id) => x.name + id
      case Label(l, e1) => s"${quote(l)} where ${quote(l)} -> ${quote(e1)}"
      case Eq(e1, e2) => s"${quote(e1)} = ${quote(e2)}"
      case IfThenElse(e1, e2, e3) => s"if ${quote(e1)} then ${quote(e2)} else ${quote(e3)}"
      case _ => "<unknown>"
    }
  }

  /**
    * Simple Scala evaluator
    */
  object Evaluator {
    val ctx = collection.mutable.HashMap[Sym[_], Any]()

    def eval[A](e: Expr[A]): A = e match {
      case ForeachUnion(x, e1, e2) =>
        val r = eval(e1).flatMap { x1 => ctx(x) = x1; eval(e2) }.asInstanceOf[A]
        ctx.remove(x)
        r
      case Union(e1, e2) => eval(e1) ++ eval(e2)
      case Singleton(e1) => List(eval(e1))
      case TupleStruct1(e1) => Tuple1(eval(e1))
      case TupleStruct2(e1, e2) => Tuple2(eval(e1), eval(e2))
      case TupleStruct3(e1, e2, e3) => Tuple3(eval(e1), eval(e2), eval(e3))
      case Project(e1, pos) =>
        eval(e1).asInstanceOf[Product].productElement(pos-1).asInstanceOf[A]
      case Eq(e1, e2) => eval(e1) == eval(e2)
      case IfThenElse(e1, e2, e3) => 
        if (eval(e1).asInstanceOf[Boolean]) 
          eval(e2).asInstanceOf[A] 
        else eval(e3).asInstanceOf[A]
      case Relation(_, c) => c
      case Const(c) => c
      case s @ Sym(_, _) => ctx(s).asInstanceOf[A]
      case _ => sys.error("not implemented")
    }
  }

  /**
    * Shredding transformation:
    * the structure of a given expression remains unchanged,
    * only input-dependent inner bags are replaced by labels
    */
  object Shredder {
    def shred[A](e: Expr[A]): Expr[A] = e match {
      case ForeachUnion(x, e1, e2) =>
        ForeachUnion(x, shred(e1), shred(e2))
      case Union(e1, e2) =>
        Union(shred(e1), shred(e2))
      case Singleton(e1) =>
        Singleton(shredSingleton(e1))
      case TupleStruct1(e1) =>
        TupleStruct1(shred(e1))
      case TupleStruct2(e1, e2) =>
        TupleStruct2(shred(e1), shred(e2))
      case TupleStruct3(e1, e2, e3) =>
        TupleStruct3(shred(e1), shred(e2), shred(e3))
      case Project(e1, pos) =>
        Project(shred(e1), pos)
      case r @ Relation(_, _) => shredRelation(r)
      case Const(_) | Sym(_, _) => e
      case _ => sys.error("not supported")
    }

    def shredSingleton[A](e: Expr[A]): Expr[A] = e match {
      case TupleStruct1(e1) =>
        TupleStruct1(
          wrapNewLabel(shred(e1))
        )
      case TupleStruct2(e1, e2) =>
        TupleStruct2(
          wrapNewLabel(shred(e1)),
          wrapNewLabel(shred(e2))
        )
      case TupleStruct3(e1, e2, e3) =>
        TupleStruct3(
          wrapNewLabel(shred(e1)),
          wrapNewLabel(shred(e2)),
          wrapNewLabel(shred(e3))
        )
      case _ => shred(e)
    }

    def wrapNewLabel[A: TypeTag](e: Expr[A]): Expr[A] =
      if (isShreddable(e)) Label(Sym[A]('l), e) else e

    def isShreddable[A: TypeTag](e: Expr[A]): Boolean =
      e.containsRelation    // shred or not

    def shredRelation[A](r: Relation[A]) = {
      // TODO: shredding of input relations
      r
    }
  }
}
